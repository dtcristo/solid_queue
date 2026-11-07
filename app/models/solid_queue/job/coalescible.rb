# frozen_string_literal: true

module SolidQueue
  class Job
    module Coalescible
      extend ActiveSupport::Concern

      MAX_COALESCE_DEADLOCK_RETRIES = 5

      included do
        has_many :coalesced_calls

        # Before anything else, so the key is locked before the job, as in admission.
        # Left to the cascade, they'd lock in the opposite order and deadlock.
        before_destroy :release_coalesce_key, prepend: true

        # Before the update, for the same reason. A key always means a live job: one
        # left on a job that finished without starting, because its class stopped
        # coalescing, would accept calls that never run.
        before_update :release_coalesce_key, if: :will_save_change_to_finished_at?
      end

      class_methods do
        # Returns the job if the call created one, nil if it was discarded into the open job.
        def enqueue_coalesced(active_job)
          ensure_coalescible
          key = raising_enqueue_errors { active_job.coalesce_key }

          enqueue_coalesced_calls(key, [ active_job ], wrap_errors: true)
        end

        # So a key or a payload that can't be built raises before anything is written.
        # A repeated :discard call is never stored, so its arguments don't have to
        # serialize, as with perform_later. The first one's only get checked here,
        # because its job is built later, if there's no open job to find. Every
        # :collect call is stored, so each one is serialized here, once.
        def prepare_all_coalesced(active_jobs)
          return [] if active_jobs.empty?

          ensure_coalescible

          active_jobs.group_by(&:coalesce_key).sort_by(&:first).map do |key, calls|
            if calls.first.class.coalesce_collect?
              [ key, calls, calls.map(&:serialize) ]
            else
              calls.first.serialize
              [ key, calls ]
            end
          end
        end

        def enqueue_all_coalesced(prepared_calls)
          prepared_calls.each { |key, calls, payloads| enqueue_coalesced_calls(key, calls, payloads) }
        end

        # InnoDB can pick one of these transactions as a deadlock victim. Admissions
        # waiting on a key that doesn't exist yet hold gap locks that block each other,
        # creating a job can do the same on a new semaphore, and a job being destroyed
        # cascades into the key's indexes in a different order. Nothing is lost by
        # running ours again, unless it's part of a bigger transaction. In that case
        # InnoDB has rolled all of it back, and only the caller can run it again.
        def retrying_deadlocks
          nested = connection_pool.with_connection(&:transaction_open?)
          attempts = 0

          begin
            yield
          rescue ActiveRecord::Deadlocked
            raise if nested || (attempts += 1) > MAX_COALESCE_DEADLOCK_RETRIES
            retry
          end
        end

        private
          def ensure_coalescible
            raise CoalesceKey::PendingMigrations unless CoalesceKey.migrated?
          end

          # Every call in active_jobs has the same key. Returns the job if a call
          # created one, nil if the calls joined the open job.
          def enqueue_coalesced_calls(key, active_jobs, payloads = nil, wrap_errors: false)
            if active_jobs.first.class.coalesce_collect?
              payloads ||= raising_enqueue_errors(wrap_errors) { active_jobs.map(&:serialize) }
              return collect_coalesced(key, active_jobs, payloads, wrap_errors: wrap_errors)
            end

            enqueued_job, open_job_id = raising_enqueue_errors(wrap_errors) do
              retrying_deadlocks { admit_discarding(key, active_jobs) }
            end

            record_coalesced(key, active_jobs, enqueued_job&.persisted?, open_job_id)

            enqueued_job
          end

          # A retry of a collecting job is a new job with the same active_job_id. The
          # calls it owes move to it in the same transaction, so the old job finishes
          # owning nothing. The block creates the new job.
          def continuing_coalesced_run(active_job)
            previous_job_id = active_job.provider_job_id
            return yield unless previous_job_id && active_job.class.coalesce_collect? && CoalesceKey.migrated?

            raising_enqueue_errors do
              transaction do
                yield.tap do |job|
                  CoalescedCall.where(job_id: previous_job_id).update_all(job_id: job.id) if job.persisted?
                end
              end
            end
          end

          # A retry of a coalescing job is the same work continuing. Active Job marks
          # it as enqueued once we return, so only raising stops on_conflict: :discard
          # from silently dropping it.
          def ensure_coalesced_retry_enqueued(active_job, enqueued_job)
            if active_job.class.coalesce_key && !enqueued_job.persisted?
              raise EnqueueError, "The retry of #{active_job.class.name} (#{active_job.job_id}) was discarded by its concurrency limit"
            end
          end

          def raising_enqueue_errors(wrap_errors = true)
            yield
          rescue ActiveRecord::ActiveRecordError => e
            raise unless wrap_errors

            enqueue_error = EnqueueError.new("#{e.class.name}: #{e.message}").tap do |error|
              error.set_backtrace e.backtrace
            end
            raise enqueue_error
          end

          # The first call creates the job if nobody holds the key, and the rest
          # are discarded into whichever job does.
          #
          # A key with calls belongs to a job from when the class collected. A call
          # can't count on that job, which fails without running it, so it seals the
          # job and tries again. Sealing is outside the transaction that found it: every
          # call waiting on the key holds the same share lock, and upgrading it deadlocks
          # with all of them. Inside the caller's own transaction it can't be outside,
          # so that deadlock can reach the caller, as in retrying_deadlocks.
          def admit_discarding(key, active_jobs)
            loop do
              admitted = transaction { admit_unless_collecting(key, active_jobs) }
              return admitted if admitted

              transaction { CoalesceKey.seal_collecting(key) }
            end
          end

          def admit_unless_collecting(key, active_jobs)
            coalesce_key = CoalesceKey.admit(key)

            unless coalesce_key.collecting?
              if coalesce_key.job_id.nil?
                attributes = attributes_from_active_job(active_jobs.first)
                enqueued_job = create_for_coalesce_key(coalesce_key, **attributes, arguments: attributes[:arguments].merge("solid_queue_coalesce_on_conflict" => "discard"))
              end

              [ enqueued_job, coalesce_key.job_id ]
            end
          end

          def create_for_coalesce_key(coalesce_key, **attributes)
            create!(**attributes).tap do |job|
              # limits_concurrency with on_conflict: :discard destroys the job right away
              if job.persisted?
                coalesce_key.update_columns(job_id: job.id)
              else
                coalesce_key.destroy!
              end
            end
          end

          # Takes one job's worth of calls at a time, each in its own transaction.
          # A call's outcome is set once its transaction is over, so a deadlock retry
          # can't leave one behind. Serialized once and before any job exists, so the
          # first call's payload is like every other call's, and a retry doesn't take
          # a new snapshot.
          def collect_coalesced(key, active_jobs, payloads, wrap_errors:)
            max_calls = active_jobs.first.class.coalesce_max_calls
            pending = active_jobs.zip(payloads)
            enqueued_job = nil

            until pending.empty?
              admitted, created_job, job_id = raising_enqueue_errors(wrap_errors) do
                retrying_deadlocks { transaction { admit_collected(key, pending, max_calls) } }
              end

              record_coalesced(key, pending.shift(admitted).map(&:first), created_job.present?, job_id)
              enqueued_job ||= created_job
            end

            enqueued_job
          end

          # Returns how many of the pending calls went in, and where. It can be none:
          # a smaller max_calls can leave the open job over its capacity, and a job
          # from when the class discarded has none, and that only seals it. The first
          # call creates the job, so its schedule, queue, priority and concurrency key
          # are the job's.
          def admit_collected(key, pending, max_calls)
            coalesce_key = CoalesceKey.admit_exclusively(key)
            calls = pending.first([ coalesce_key.capacity(max_calls), 0 ].max)

            if calls.empty?
              coalesce_key.destroy!
              [ 0 ]
            else
              if coalesce_key.job_id.nil?
                active_job, payload = calls.first
                created_job = create!(**attributes_from_active_job(active_job, arguments: payload.merge("solid_queue_coalesce_on_conflict" => "collect")))
              end
              job_id = coalesce_key.job_id || created_job.id

              CoalescedCall.insert_all(calls.map { |_, payload| { job_id: job_id, arguments: payload } })
              coalesce_key.fill(job_id, calls.size, max_calls)

              [ calls.size, created_job, job_id ]
            end
          end

          # A call has an outcome only if it was admitted: when on_conflict: :discard
          # destroys the job that was just created, nothing holds the key. The first
          # call is the one that creates a job.
          def record_coalesced(key, active_jobs, created, job_id)
            active_jobs.each do |active_job|
              status = if job_id.nil? then nil
              elsif created && active_job.equal?(active_jobs.first) then :created
              elsif active_job.class.coalesce_collect? then :collected
              else :discarded
              end

              active_job.provider_job_id = job_id
              active_job.successfully_enqueued = status.in?(%i[ created collected ])
              active_job.send(:coalesce_status=, status)
            end

            # After every outcome, because a subscriber can raise, and a call without
            # one would look like it needs enqueuing again.
            if job_id
              active_jobs.each do |active_job|
                SolidQueue.instrument(:coalesce, status: active_job.coalesce_status, key: key, job_id: job_id, active_job_id: active_job.job_id, class_name: active_job.class.name)
              end
            end
          end
      end

      # The start step. Returns what the Active Job needs to know about this run,
      # which stays in memory instead of going into its serialized data. A
      # discarding job needs nothing, but it's truthy, so retry_job knows the
      # run is a coalesced one.
      #
      # Releasing commits before the calls are read, in a statement of their own.
      # The read that finds the key in lock_for_release sets the snapshot of a MySQL
      # REPEATABLE READ transaction, so a read in the same transaction could miss
      # calls committed while it waited for the lock. The concurrency key is kept in
      # case the arguments it was computed from can't be deserialized anymore.
      def start_coalesced_run
        return unless coalesced? && CoalesceKey.migrated?

        self.class.retrying_deadlocks { transaction { release_coalesce_key } }

        if created_collecting?
          { "calls" => coalesced_calls.order(:id).pluck(:id, :arguments), "concurrency_key" => concurrency_key }
        else
          {}
        end
      end

      # How the job was created, which outlives its class's declaration
      def created_collecting?
        arguments.is_a?(Hash) && arguments["solid_queue_coalesce_on_conflict"] == "collect"
      end

      # lock_for_release won't touch a newer job's key
      def release_coalesce_key
        CoalesceKey.lock_for_release(id)&.destroy! if may_hold_coalesce_key?
      end

      private
        # Whether this job's class coalesces
        def coalesced?
          !!job_class.try(:coalesce_key)
        end

        # The marker covers a job whose class stopped coalescing, and the class covers
        # a job that lost its marker. Anything that can't answer gets the read, so that
        # failing or discarding a job still works when its payload or class won't load.
        def may_hold_coalesce_key?
          !arguments.is_a?(Hash) || arguments.key?("solid_queue_coalesce_on_conflict") || coalesced?
        rescue StandardError, ScriptError
          true
        end
    end
  end
end
