# frozen_string_literal: true

module SolidQueue
  class Job
    module Coalescible
      extend ActiveSupport::Concern

      MAX_COALESCE_DEADLOCK_RETRIES = 5

      included do
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
        # A repeated call is never stored, so its arguments don't have to serialize, as
        # with perform_later. The first one's only get checked here, because its job is
        # built later, if there's no open job to find.
        def prepare_all_coalesced(active_jobs)
          return [] if active_jobs.empty?

          ensure_coalescible

          active_jobs.group_by(&:coalesce_key).sort_by(&:first).map do |key, calls|
            calls.first.serialize
            [ key, calls ]
          end
        end

        def enqueue_all_coalesced(prepared_calls)
          prepared_calls.each { |key, calls| enqueue_coalesced_calls(key, calls) }
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
          def enqueue_coalesced_calls(key, active_jobs, wrap_errors: false)
            enqueued_job, open_job_id = raising_enqueue_errors(wrap_errors) do
              retrying_deadlocks { admit_discarding(key, active_jobs) }
            end

            record_coalesced(key, active_jobs, enqueued_job&.persisted?, open_job_id)

            enqueued_job
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
          def admit_discarding(key, active_jobs)
            transaction do
              coalesce_key = CoalesceKey.admit(key)

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

          # A call has an outcome only if it was admitted: when on_conflict: :discard
          # destroys the job that was just created, nothing holds the key. The first
          # call is the one that creates a job.
          def record_coalesced(key, active_jobs, created, job_id)
            active_jobs.each do |active_job|
              status = if job_id.nil? then nil
              elsif created && active_job.equal?(active_jobs.first) then :created
              else :discarded
              end

              active_job.provider_job_id = job_id
              active_job.successfully_enqueued = status == :created
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
      def start_coalesced_run
        return unless coalesced? && CoalesceKey.migrated?

        self.class.retrying_deadlocks { transaction { release_coalesce_key } }

        {}
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
