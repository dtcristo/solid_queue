# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceValidationTest < ActiveSupport::TestCase
  class SimpleRecurringJob < ApplicationJob
    coalesce key: "recurring"

    def perform
      JobBuffer.add("ran")
    end
  end

  test "key is required" do
    assert_raises(ArgumentError) { new_job_class { coalesce } }
  end

  test "key is the only option" do
    %i[ on_conflict group duration arguments debounce max_wait max_calls on_full ].each do |option|
      assert_raises(ArgumentError, option.inspect) { new_job_class { coalesce key: "a", option => :discard } }
    end
  end

  test "coalesce composes with limits_concurrency, whichever comes first and whatever happens on a conflict" do
    %i[ block discard ].each do |on_conflict|
      assert new_job_class {
        coalesce key: "a"
        limits_concurrency key: "b", on_conflict: on_conflict
      }

      assert new_job_class {
        limits_concurrency key: "b", on_conflict: on_conflict
        coalesce key: "a"
      }
    end
  end

  test "a recurring task for a coalescing job is valid and a suppressed tick records nothing" do
    task = SolidQueue::RecurringTask.new(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")
    assert task.valid?
    task.save!

    first = task.enqueue(at: 2.hours.ago)
    assert first.successfully_enqueued?
    assert_equal :created, first.coalesce_status
    assert_equal 1, SolidQueue::RecurringExecution.count

    second = task.enqueue(at: 1.hour.ago)
    assert_equal :discarded, second.coalesce_status
    assert_equal first.provider_job_id, second.provider_job_id
    assert_not second.successfully_enqueued?
    assert_equal 1, SolidQueue::RecurringExecution.count
  end

  test "a tick that collides with the open job doesn't raise or roll anything back" do
    task = SolidQueue::RecurringTask.create!(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")
    run_at = 1.hour.ago

    first = task.enqueue(at: run_at)

    # Another scheduler taking the same tick
    assert_nothing_raised { assert_not task.enqueue(at: run_at).successfully_enqueued? }

    assert_equal [ first.provider_job_id ], SolidQueue::RecurringExecution.where(task_key: "simple", run_at: run_at).pluck(:job_id)
    assert_equal [ first.provider_job_id ], SolidQueue::CoalesceKey.pluck(:job_id)
    assert_equal [ first.provider_job_id ], SolidQueue::Job.pluck(:id)

    perform_ready_jobs
    assert_equal [ "ran" ], JobBuffer.values
  end

  test "a tick after the open job has started enqueues and records again" do
    task = SolidQueue::RecurringTask.create!(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")

    task.enqueue(at: 2.hours.ago)
    perform_ready_jobs

    assert task.enqueue(at: 1.hour.ago).successfully_enqueued?
    assert_equal 2, SolidQueue::RecurringExecution.count
  end

  private
    def new_job_class(&block)
      Class.new(ApplicationJob, &block)
    end
end
