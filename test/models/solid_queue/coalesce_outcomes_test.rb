# frozen_string_literal: true

require "test_helper"

# What a call can read back from its own instance, one test per row of the table
# in the README. Tests elsewhere pin the outcomes that come with a whole behaviour.
class SolidQueue::CoalesceOutcomesTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class AdapterWithItsOwnIds
    def enqueue(active_job)
      active_job.provider_job_id = "adapter-id"
    end

    def enqueue_at(active_job, _timestamp)
      enqueue(active_job)
    end

    def enqueue_after_transaction_commit?
      false
    end
  end

  class OtherAdapterJob < ApplicationJob
    self.queue_adapter = AdapterWithItsOwnIds.new

    coalesce key: "other"

    def perform
    end
  end

  class PlainJob < ApplicationJob
    def perform
    end
  end

  class AbortingJob < ApplicationJob
    coalesce key: ->(name) { name }
    before_enqueue { throw :abort if self.class.abort }

    cattr_accessor :abort, default: false

    def perform(name)
    end
  end

  # Rails 7.2 defers by default with Solid Queue, which a rollback would drop
  class NotDeferredDiscardingJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: "not_deferred"

    def perform
    end
  end

  class WorkerDiscardingJob < ApplicationJob
    coalesce key: ->(name) { name }
    rescue_from(ExpectedTestError) { retry_job.then { |retried| JobBuffer.add([ :retried, retried.coalesce_status, retried.provider_job_id, retried.successfully_enqueued? ]) } }

    def perform(_name)
      JobBuffer.add([ coalesce_status, provider_job_id, successfully_enqueued?.present? ])
      raise ExpectedTestError if executions == 1
    end
  end

  teardown { AbortingJob.abort = false }

  test "a call on a class that doesn't coalesce has no status" do
    active_job = PlainJob.perform_later

    assert_outcome active_job, nil, SolidQueue::Job.sole.id, true
  end

  test "a before_enqueue abort has no outcome, and perform_later returns false" do
    AbortingJob.abort = true
    active_job = nil

    assert_equal false, AbortingJob.perform_later("a") { |job| active_job = job }

    assert_outcome active_job, nil, nil, false
    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "an abort on a reused instance keeps its previous outcome" do
    active_job = AbortingJob.perform_later("a")
    job_id = active_job.provider_job_id
    AbortingJob.abort = true

    assert_equal false, active_job.enqueue

    assert_outcome active_job, :created, job_id, false
  end

  test "a failed enqueue of a fresh instance raises, and has no outcome" do
    SolidQueue::CoalesceKey.stubs(:admit).raises(ActiveRecord::StatementInvalid, "boom")
    active_job = CoalescedRefreshJob.new("a")

    assert_raises(SolidQueue::Job::EnqueueError) { active_job.enqueue }

    assert_outcome active_job, nil, nil, false
  end

  test "a failed enqueue of a reused instance keeps its previous outcome" do
    active_job = CoalescedRefreshJob.perform_later("a")
    job_id = active_job.provider_job_id
    SolidQueue::CoalesceKey.stubs(:admit).raises(ActiveRecord::StatementInvalid, "boom")

    assert_raises(SolidQueue::Job::EnqueueError) { active_job.enqueue }

    assert_outcome active_job, :created, job_id, false
  end

  test "rolling back your own transaction leaves a single call's outcome stale" do
    created, discarded = nil

    SolidQueue::Record.transaction do
      created = NotDeferredDiscardingJob.perform_later
      discarded = NotDeferredDiscardingJob.perform_later
      raise ActiveRecord::Rollback
    end

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal [ :created, :discarded ], [ created, discarded ].map(&:coalesce_status)
    assert created.provider_job_id
    assert_equal created.provider_job_id, discarded.provider_job_id
    assert created.successfully_enqueued?
    assert discarded.successfully_enqueued?
  end

  test "a call on another adapter has no status, and the adapter's own ID" do
    assert_outcome OtherAdapterJob.perform_later, nil, "adapter-id", true
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "perform_now on a new instance has no outcome, and stores nothing" do
    active_job = CoalescedRefreshJob.new("a")
    active_job.perform_now

    assert_outcome active_job, nil, nil, false
    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a job's instance on a worker has no status, on the run and on its retry" do
    enqueued = WorkerDiscardingJob.perform_later("a")
    first_job_id = enqueued.provider_job_id

    2.times { perform_ready_jobs }

    retry_job_id = SolidQueue::Job.where.not(id: first_job_id).sole.id
    assert_equal [ [ nil, first_job_id, false ], [ :retried, nil, retry_job_id, true ], [ nil, retry_job_id, false ] ], JobBuffer.values
  end

  private
    def assert_outcome(active_job, status, provider_job_id, enqueued)
      status.nil? ? assert_nil(active_job.coalesce_status) : assert_equal(status, active_job.coalesce_status)
      provider_job_id.nil? ? assert_nil(active_job.provider_job_id) : assert_equal(provider_job_id, active_job.provider_job_id)
      # Before an enqueue, Rails versions disagree on false or nil
      assert_equal enqueued, active_job.successfully_enqueued?.present?
    end
end
