# frozen_string_literal: true

module SolidQueue
  class Job < Record
    class EnqueueError < StandardError; end

    # Raised when a job's class can't be resolved anymore, typically because it
    # was renamed or removed in a deploy while jobs referencing it were in
    # flight. It subclasses NameError, which is what resolving the class raises.
    class ClassMissingError < NameError
      def self.for(job)
        new("Job class #{job.class_name.inspect} could not be resolved")
      end
    end

    include Executable, Clearable, Recurrable, Batchable, Coalescible

    serialize :arguments, coder: JSON

    class << self
      def enqueue_all(active_jobs)
        # Bulk enqueues bypass ActiveJob#enqueue, so batch membership is captured here
        current_batch_id = Batch.current_batch_id

        active_jobs.each do |job|
          job.scheduled_at ||= Time.current
          job.batch_id = current_batch_id || job.batch_id
        end

        coalesced_jobs, active_jobs_to_create = active_jobs.partition(&:coalesced_enqueue?)
        coalesced_calls = prepare_all_coalesced(coalesced_jobs)
        active_jobs_by_job_id = active_jobs_to_create.index_by(&:job_id)

        if active_jobs_to_create.any?
          transaction do
            jobs = create_all_from_active_jobs(active_jobs_to_create)
            prepare_all_for_execution(jobs).tap do |enqueued_jobs|
              enqueued_jobs.each do |enqueued_job|
                active_jobs_by_job_id[enqueued_job.active_job_id].provider_job_id = enqueued_job.id
                active_jobs_by_job_id[enqueued_job.active_job_id].successfully_enqueued = true
              end
            end
          end
        end

        enqueue_all_coalesced(coalesced_calls)

        active_jobs.count(&:successfully_enqueued?)
      end

      def enqueue(active_job, scheduled_at: Time.current)
        active_job.scheduled_at = scheduled_at
        return enqueue_coalesced(active_job) if active_job.coalesced_enqueue?

        continuing_coalesced_run(active_job) { create_from_active_job(active_job) }.tap do |enqueued_job|
          active_job.provider_job_id = enqueued_job.id if enqueued_job.persisted?
          active_job.successfully_enqueued = enqueued_job.persisted?
          ensure_coalesced_retry_enqueued(active_job, enqueued_job)
        end
      end

      private
        DEFAULT_PRIORITY = 0
        DEFAULT_QUEUE_NAME = "default"

        def create_from_active_job(active_job)
          create!(**attributes_from_active_job(active_job))
        rescue ActiveRecord::ActiveRecordError => e
          enqueue_error = EnqueueError.new("#{e.class.name}: #{e.message}").tap do |error|
            error.set_backtrace e.backtrace
          end
          raise enqueue_error
        end

        def create_all_from_active_jobs(active_jobs)
          job_rows = active_jobs.map { |job| attributes_from_active_job(job) }
          insert_all(job_rows)
          where(active_job_id: active_jobs.map(&:job_id)).order(id: :asc)
        end

        def attributes_from_active_job(active_job, arguments: active_job.serialize)
          {
            queue_name: active_job.queue_name || DEFAULT_QUEUE_NAME,
            active_job_id: active_job.job_id,
            priority: active_job.priority || DEFAULT_PRIORITY,
            scheduled_at: active_job.scheduled_at,
            class_name: active_job.class.name,
            arguments: arguments,
            concurrency_key: active_job.concurrency_key
          }.tap do |attributes|
            attributes[:batch_id] = active_job.batch_id if Batch.migrated?
          end
        end
    end
  end
end
