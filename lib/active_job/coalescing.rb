# frozen_string_literal: true

module ActiveJob
  module Coalescing
    extend ActiveSupport::Concern

    included do
      class_attribute :coalesce_key, instance_accessor: false
    end

    # What happened to the call: :created, :discarded, or nil if this instance has no
    # recorded admission. More values may be added. It isn't serialized, so it's nil on
    # a worker's instance.
    attr_reader :coalesce_status

    private attr_writer :coalesce_status

    class_methods do
      def coalesce(key:)
        self.coalesce_key = key
      end
    end

    # Runs before the arguments are deserialized, and outside rescue_from, so retry_on
    # can't swallow what's raised here. ClaimedExecution#execute turns it into a
    # failed execution.
    def perform_now
      start_coalesced_run
      super
    end

    # How the job was created is stored with the job, so it survives retries and
    # outlives the class's declaration. Only the job's row gets it, when it takes its
    # key, so an instance only has it if it was read from one.
    def serialize
      @coalesce_on_conflict ? super.merge("solid_queue_coalesce_on_conflict" => @coalesce_on_conflict) : super
    end

    def deserialize(job_data)
      super
      @coalesce_on_conflict = job_data["solid_queue_coalesce_on_conflict"]
    end

    # Scoped to the job class name, and built like concurrency_key. In a batch it's
    # scoped to the batch too, so each coalesced job belongs to one batch, and a call
    # that joins it can't leave a batch waiting on another batch's job.
    def coalesce_key
      if self.class.coalesce_key
        param = compute_concurrency_parameter(self.class.coalesce_key)

        case param
        when ActiveRecord::Base
          [ coalesce_batch_scope, self.class.name, param.class.name, param.id ]
        else
          [ coalesce_batch_scope, self.class.name, param ]
        end.compact.join("/")
      end
    end

    # A retry is the same work continuing, so it can't join or suppress anything
    def coalesced_enqueue?
      self.class.coalesce_key && executions.to_i.zero?
    end

    # A retry that doesn't get enqueued has to fail the run, or the calls that were
    # discarded into this job would be left with no job to do their work.
    def retry_job(options = {})
      super.tap do |enqueued|
        if @coalesced_run && !enqueued
          raise enqueue_error || SolidQueue::Job::EnqueueError.new("Couldn't enqueue the retry of #{self.class.name} (#{job_id})")
        end
      end
    end

    private
      def coalesce_batch_scope
        [ "Batch", batch_id ] if batch_id
      end

      # Only the run of a job a worker has claimed. perform_now on an instance that
      # went through perform_later also has a provider_job_id.
      def start_coalesced_run
        if self.class.coalesce_key && provider_job_id && self.class.queue_adapter_name == "solid_queue"
          @coalesced_run = SolidQueue::Job.joins(:claimed_execution).find_by(id: provider_job_id)&.start_coalesced_run
        end
      end
  end
end
