# frozen_string_literal: true

require "test_helper"

# What a call can read back from its own instance, one test per row of the table
# in the README. Tests elsewhere pin the outcomes that come with a whole behaviour.
class SolidQueue::CoalesceCollectOutcomesTest < ActiveSupport::TestCase
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

  class NotDeferredCollectingJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: "not_deferred", on_conflict: :collect

    def perform_calls(_calls)
    end
  end

  class AfterEnqueueCollectingJob < ApplicationJob
    coalesce key: "callback", on_conflict: :collect
    after_enqueue { JobBuffer.add([ coalesce_status, provider_job_id ]) }

    def perform_calls(_calls)
    end
  end

  class WorkerCollectingJob < ApplicationJob
    coalesce key: "worker", on_conflict: :collect
    rescue_from(ExpectedTestError) { retry_job.then { |retried| JobBuffer.add([ :retried, retried.coalesce_status, retried.provider_job_id, retried.successfully_enqueued? ]) } }

    def perform_calls(calls)
      JobBuffer.add([ coalesce_status, provider_job_id, successfully_enqueued?.present?, calls.map(&:id) ])
      raise ExpectedTestError if executions == 1
    end
  end

  class ContextJob < ApplicationJob
    coalesce key: "context", on_conflict: :collect

    attr_accessor :tenant

    def serialize
      super.merge("tenant" => tenant)
    end

    def deserialize(job_data)
      super
      self.tenant = job_data["tenant"]
    end

    def perform_calls(calls)
      JobBuffer.add([ I18n.locale, Time.zone.name, tenant, calls.map(&:job_data) ])
    end
  end

  teardown { AbortingJob.abort = false }

  test "a joined call belongs to the job that held the key, whose active job ID is another call's" do
    first = CollectedCallsJob.perform_later("g", 1)
    second = CollectedCallsJob.perform_later("g", 2)

    assert_outcome second, :collected, first.provider_job_id, true
    assert_not_equal second.job_id, SolidQueue::Job.find(second.provider_job_id).active_job_id
    assert_equal first.job_id, SolidQueue::Job.find(second.provider_job_id).active_job_id
  end

  test "after_enqueue sees a collected call's status and job" do
    first = AfterEnqueueCollectingJob.perform_later
    AfterEnqueueCollectingJob.perform_later

    assert_equal [ [ :created, first.provider_job_id ], [ :collected, first.provider_job_id ] ], JobBuffer.values
  end

  test "a failed enqueue of a fresh instance raises, and has no outcome" do
    SolidQueue::CoalesceKey.stubs(:admit).raises(ActiveRecord::StatementInvalid, "boom")
    SolidQueue::CoalesceKey.stubs(:admit_exclusively).raises(ActiveRecord::StatementInvalid, "boom")

    [ CoalescedRefreshJob.new("a"), CollectedCallsJob.new("g", 1) ].each do |active_job|
      assert_raises(SolidQueue::Job::EnqueueError) { active_job.enqueue }

      assert_outcome active_job, nil, nil, false
    end
  end

  test "a failed enqueue of a reused instance keeps its previous outcome" do
    [ CoalescedRefreshJob.perform_later("a"), CollectedCallsJob.perform_later("g", 1) ].each do |active_job|
      status, job_id = active_job.coalesce_status, active_job.provider_job_id
      SolidQueue::CoalesceKey.stubs(:admit).raises(ActiveRecord::StatementInvalid, "boom")
      SolidQueue::CoalesceKey.stubs(:admit_exclusively).raises(ActiveRecord::StatementInvalid, "boom")

      assert_raises(SolidQueue::Job::EnqueueError) { active_job.enqueue }

      assert_outcome active_job, status, job_id, false
      SolidQueue::CoalesceKey.unstub(:admit, :admit_exclusively)
    end
  end

  test "rolling back your own transaction leaves a single call's outcome stale" do
    created, collected = nil

    SolidQueue::Record.transaction do
      created = NotDeferredDiscardingJob.perform_later
      NotDeferredCollectingJob.perform_later
      collected = NotDeferredCollectingJob.perform_later
      raise ActiveRecord::Rollback
    end

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal [ :created, :collected ], [ created, collected ].map(&:coalesce_status)
    assert created.provider_job_id
    assert collected.provider_job_id
    assert created.successfully_enqueued?
    assert collected.successfully_enqueued?
  end

  test "perform_now on a new instance has no outcome, and stores nothing" do
    active_job = CollectedCallsJob.new("g", 1)
    active_job.perform_now

    assert_outcome active_job, nil, nil, false
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  test "a collecting job's instance on a worker has no status, and a retry moves its calls to a new job with the same IDs" do
    first = WorkerCollectingJob.perform_later
    second = WorkerCollectingJob.perform_later
    call_ids = SolidQueue::CoalescedCall.order(:id).pluck(:id)
    first_job_id = first.provider_job_id

    perform_ready_jobs

    retry_job_id = SolidQueue::Job.where(finished_at: nil).sole.id
    assert_not_equal first_job_id, retry_job_id
    assert_equal call_ids, SolidQueue::CoalescedCall.where(job_id: retry_job_id).order(:id).pluck(:id)

    perform_ready_jobs

    assert_equal [ [ nil, first_job_id, false, call_ids ], [ :retried, nil, retry_job_id, true ], [ nil, retry_job_id, false, call_ids ] ], JobBuffer.values

    # What the enqueuing instances read is a snapshot
    assert_equal [ first_job_id, first_job_id ], [ first, second ].map(&:provider_job_id)
  end

  test "each call's locale, timezone and custom fields stay in its job_data, and the run keeps the owning job's" do
    with_available_locales(:en, :de) do
      enqueue_context_call("first", :de, "Tokyo")
      enqueue_context_call("second", :en, "UTC")

      perform_ready_jobs

      locale, zone, tenant, job_data = JobBuffer.values.sole
      assert_equal [ :de, "Tokyo", "first" ], [ locale, zone, tenant ]
      assert_equal [ [ "de", "Tokyo", "first" ], [ "en", "UTC", "second" ] ], job_data.map { |data| data.values_at("locale", "timezone", "tenant") }
    end
  end

  private
    def assert_outcome(active_job, status, provider_job_id, enqueued)
      status.nil? ? assert_nil(active_job.coalesce_status) : assert_equal(status, active_job.coalesce_status)
      provider_job_id.nil? ? assert_nil(active_job.provider_job_id) : assert_equal(provider_job_id, active_job.provider_job_id)
      # Before an enqueue, Rails versions disagree on false or nil
      assert_equal enqueued, active_job.successfully_enqueued?.present?
    end

    def enqueue_context_call(tenant, locale, zone)
      I18n.with_locale(locale) do
        Time.use_zone(zone) { ContextJob.new.tap { |job| job.tenant = tenant }.enqueue }
      end
    end

    def with_available_locales(*locales)
      previous = I18n.available_locales
      I18n.available_locales = locales
      yield
    ensure
      I18n.available_locales = previous
    end
end
