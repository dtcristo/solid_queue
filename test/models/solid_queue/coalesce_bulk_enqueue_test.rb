# frozen_string_literal: true

require "test_helper"

# The contract of a bulk enqueue that has coalesced calls in it, as the README
# states it: what's written, in what order, and what each call reports.
class SolidQueue::CoalesceBulkEnqueueTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class OrdinaryJob < ApplicationJob
    def perform(*); end
  end

  class DiscardJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class RaisingKeyJob < ApplicationJob
    coalesce key: ->(*) { raise ExpectedTestError, "no key" }

    def perform(*); end
  end

  class LimitedDiscardJob < ApplicationJob
    coalesce key: ->(group, *) { group }
    limits_concurrency key: ->(_group, i) { i == :poison ? raise(ExpectedTestError, "no concurrency key") : i }

    def perform(*); end
  end

  class ConflictDiscardJob < ApplicationJob
    coalesce key: ->(group, *) { group }
    limits_concurrency key: "shared", on_conflict: :discard

    def perform(*); end
  end

  test "mixed jobs are stored ordinary first, then by key, and each call reports its own outcome" do
    first_ordinary = OrdinaryJob.new(1)
    second_ordinary = OrdinaryJob.new(2)
    discard_w = DiscardJob.new("w")
    discard_x = 3.times.map { |i| DiscardJob.new("x", i) }
    discard_y = DiscardJob.new("y")
    jobs = [ discard_y, discard_x[0], first_ordinary, discard_w, discard_x[1], second_ordinary, discard_x[2] ]

    events = capture_events { assert_equal 5, SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ first_ordinary, second_ordinary, discard_w, discard_x[0], discard_y ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal [ nil, nil ], [ first_ordinary, second_ordinary ].map(&:coalesce_status)
    assert_equal [ :created, :discarded, :discarded ], discard_x.map(&:coalesce_status)
    assert_equal [ :created, :created ], [ discard_w, discard_y ].map(&:coalesce_status)

    assert_equal jobs - discard_x.drop(1), jobs.select(&:successfully_enqueued?)
    assert_equal [ discard_x[0].provider_job_id ] * 3, discard_x.map(&:provider_job_id)
    (jobs - discard_x.drop(1)).each do |active_job|
      assert_equal active_job.job_id, SolidQueue::Job.find(active_job.provider_job_id).active_job_id
    end
    assert_equal [ "x", 0 ], SolidQueue::Job.find(discard_x[0].provider_job_id).arguments["arguments"]

    assert_equal [ discard_w, *discard_x, discard_y ].map(&:job_id), events.map { |event| event.payload[:active_job_id] }
  end

  test "a coalesce key that raises fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(1), DiscardJob.new("b"), RaisingKeyJob.new ]

    assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "an unserializable argument on a key's first call fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(1), DiscardJob.new("a", 0), DiscardJob.new("b", Object.new) ]

    assert_raises(ActiveJob::SerializationError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "an unserializable argument on a repeated call stays silently discarded" do
    jobs = [ DiscardJob.new("a", 0), DiscardJob.new("a", Object.new) ]

    assert_equal 1, SolidQueue::Job.enqueue_all(jobs)

    assert_equal [ :created, :discarded ], jobs.map(&:coalesce_status)
    assert_equal [ jobs.first.provider_job_id ] * 2, jobs.map(&:provider_job_id)
    assert_equal [ "a", 0 ], SolidQueue::Job.sole.arguments["arguments"]
  end

  test "a raising subscriber leaves every call of that key with its outcome, and later keys' calls with none" do
    first_calls = 2.times.map { DiscardJob.new("a") }
    later = DiscardJob.new("b")

    with_raising_subscriber(on_call: 1) do
      assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(first_calls + [ later ]) }
    end

    assert_equal [ :created, :discarded ], first_calls.map(&:coalesce_status)
    assert_equal [ first_calls.first.provider_job_id ] * 2, first_calls.map(&:provider_job_id)
    assert_equal [ true, false ], first_calls.map { |call| call.successfully_enqueued? || false }
    assert_nil later.provider_job_id
    assert_nil later.coalesce_status
    assert_equal 1, SolidQueue::Job.count
  end

  test "an ordinary job that can't serialize fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(Object.new), DiscardJob.new("a") ]

    assert_raises(ActiveJob::SerializationError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "a database failure on the ordinary jobs raises as is, with nothing written" do
    jobs = mixed_jobs
    SolidQueue::Job.stubs(:insert_all).raises(ActiveRecord::StatementInvalid, "boom")

    assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "a failure on an admission keeps earlier work, and later calls have no outcome" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind

    failing(SolidQueue::Job, :create!, matching: creating(discard_b.first)) do
      assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all([ ordinary, *discard_a, *discard_b, discard_c ]) }
    end

    assert_equal [ :created, :discarded ], discard_a.map(&:coalesce_status)
    assert_equal [ nil, nil, nil ], [ *discard_b, discard_c ].map(&:provider_job_id)
    assert_equal [ nil, nil, nil ], [ *discard_b, discard_c ].map(&:coalesce_status)
    assert_equal [ discard_a.first.coalesce_key ], SolidQueue::CoalesceKey.pluck(:key)
    assert_equal [ ordinary, discard_a.first ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert ordinary.successfully_enqueued?
  end

  test "a failure partway through emits events only for the calls that committed" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind

    events = capture_events do
      failing(SolidQueue::Job, :create!, matching: creating(discard_b.first)) do
        assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all([ ordinary, *discard_a, *discard_b, discard_c ]) }
      end
    end

    assert_equal discard_a.map(&:job_id), events.map { |event| event.payload[:active_job_id] }
    assert_equal [ :created, :discarded ], events.map { |event| event.payload[:status] }
  end

  test "a concurrency key that raises on a later key's first call leaves exact outcomes" do
    jobs = [ LimitedDiscardJob.new("g", 0), LimitedDiscardJob.new("g", 1), LimitedDiscardJob.new("h", :poison), LimitedDiscardJob.new("i", 3) ]

    assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ :created, :discarded, nil, nil ], jobs.map(&:coalesce_status)
    assert_equal [ jobs.first.job_id ], SolidQueue::Job.pluck(:active_job_id)
    assert_equal [ jobs.first.coalesce_key ], SolidQueue::CoalesceKey.pluck(:key)
  end

  test "re-enqueuing the calls without a provider job ID stores each one once" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind
    jobs = [ ordinary, *discard_a, *discard_b, discard_c ]

    failing(SolidQueue::Job, :create!, matching: creating(discard_b.first)) do
      assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all(jobs) }
    end

    SolidQueue::Job.enqueue_all(jobs.reject(&:provider_job_id))

    assert_equal [ ordinary, discard_a.first, discard_b.first, discard_c ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal [ :created, :discarded ], discard_b.map(&:coalesce_status)
    assert_equal 3, SolidQueue::CoalesceKey.count
  end

  test "re-enqueuing after a raising subscriber stores nothing twice" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind
    jobs = [ ordinary, *discard_a, *discard_b, discard_c ]

    with_raising_subscriber(on_call: 2) do
      assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(jobs) }
    end

    SolidQueue::Job.enqueue_all(jobs.reject(&:provider_job_id))

    assert_equal 1, SolidQueue::Job.where(active_job_id: ordinary.job_id).count
    assert_equal 3, SolidQueue::Job.where(class_name: DiscardJob.name).count
    assert_equal [ ordinary, discard_a.first, discard_b.first, discard_c ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
  end

  test "re-enqueuing a call a concurrency conflict destroyed has no effect while the conflict lasts" do
    ConflictDiscardJob.perform_later("a")
    destroyed = ConflictDiscardJob.new("b")

    SolidQueue::Job.enqueue_all([ destroyed ])
    assert_nil destroyed.provider_job_id

    assert_no_difference -> { SolidQueue::Job.count + SolidQueue::CoalesceKey.count } do
      SolidQueue::Job.enqueue_all([ destroyed ].reject(&:provider_job_id))
    end
  end

  test "a failed re-enqueue of the same instance keeps showing its old outcome" do
    call = DiscardJob.new("a")
    SolidQueue::Job.enqueue_all([ call ])
    job_id = call.provider_job_id
    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "boom")
    SolidQueue::Job.find(job_id).destroy!

    assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all([ call ]) }

    assert_equal job_id, call.provider_job_id
    assert_equal :created, call.coalesce_status
  end

  test "deadlocks in a mixed bulk enqueue retry each admission without repeating earlier work" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind
    jobs = [ ordinary, *discard_a, *discard_b, discard_c ]

    failing(SolidQueue::Job, :create!, matching: creating(discard_b.first), error: ActiveRecord::Deadlocked) do
      assert_equal 4, SolidQueue::Job.enqueue_all(jobs)
    end

    assert_equal [ ordinary, discard_a.first, discard_b.first, discard_c ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal [ :created, :discarded ], discard_a.map(&:coalesce_status)
    assert_equal [ :created, :discarded ], discard_b.map(&:coalesce_status)
    assert_equal 3, SolidQueue::CoalesceKey.count
  end

  test "rolling back an enclosing transaction leaves no rows, and outcomes and events as they were" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind
    jobs = [ ordinary, *discard_a, *discard_b, discard_c ]

    events = capture_events do
      SolidQueue::Record.transaction do
        SolidQueue::Job.enqueue_all(jobs)
        raise ActiveRecord::Rollback
      end
    end

    assert_nothing_stored
    assert jobs.all?(&:provider_job_id)
    assert ordinary.successfully_enqueued?
    assert_equal [ :created, :discarded, :created, :discarded, :created ], [ *discard_a, *discard_b, discard_c ].map(&:coalesce_status)
    assert_equal [ *discard_a, *discard_b, discard_c ].map(&:job_id), events.map { |event| event.payload[:active_job_id] }
  end

  test "a failure inside an enclosing transaction rolls back everything" do
    ordinary, discard_a, discard_b, discard_c = mixed_jobs_by_kind

    failing(SolidQueue::Job, :create!, matching: creating(discard_b.first)) do
      assert_raises(ActiveRecord::StatementInvalid) do
        SolidQueue::Record.transaction { SolidQueue::Job.enqueue_all([ ordinary, *discard_a, *discard_b, discard_c ]) }
      end
    end

    assert_nothing_stored
  end

  private
    def mixed_jobs_by_kind
      [ OrdinaryJob.new(1), 2.times.map { DiscardJob.new("a") }, 2.times.map { DiscardJob.new("b") }, DiscardJob.new("c") ]
    end

    def mixed_jobs
      mixed_jobs_by_kind.flatten
    end

    # Runs the method, then raises once as if it had failed, so its transaction has
    # something to roll back. It has to raise, or the test wouldn't cover the failure.
    def failing(receiver, method_name, matching:, error: ActiveRecord::StatementInvalid)
      original = receiver.method(method_name)
      failed = false

      receiver.define_singleton_method(method_name) do |*args, **options, &block|
        original.call(*args, **options, &block).tap do
          if !failed && matching.call(*args, **options)
            failed = true
            raise error, "boom"
          end
        end
      end

      yield

      assert failed, "#{receiver}.#{method_name} never got the call that was meant to fail"
    ensure
      receiver.singleton_class.remove_method(method_name)
    end

    def creating(call)
      ->(active_job_id:, **) { active_job_id == call.job_id }
    end

    def capture_events
      events = []
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") { |*args| events << ActiveSupport::Notifications::Event.new(*args) }
      yield
      events
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def with_raising_subscriber(on_call:)
      calls = 0
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do
        raise ExpectedTestError, "subscriber" if (calls += 1) == on_call
      end

      yield
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def assert_nothing_stored
      assert_equal 0, SolidQueue::Job.count
      assert_equal 0, SolidQueue::CoalesceKey.count
    end

    def assert_nothing_written(jobs)
      assert_nothing_stored
      assert_equal [ nil ] * jobs.size, jobs.map(&:provider_job_id)
      assert_equal [ nil ] * jobs.size, jobs.map(&:coalesce_status)
    end
end
