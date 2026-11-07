# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceValidationTest < ActiveSupport::TestCase
  class CollectingRecurringJob < ApplicationJob
    coalesce key: "recurring", on_conflict: :collect

    def perform_calls(calls)
    end
  end

  class SimpleRecurringJob < ApplicationJob
    coalesce key: "recurring"

    def perform
      JobBuffer.add("ran")
    end
  end

  test "key is required" do
    assert_raises(ArgumentError) { new_job_class { coalesce } }
  end

  test "key, on_conflict and max_calls are the only options" do
    %i[ group duration arguments debounce max_wait on_full ].each do |option|
      assert_raises(ArgumentError, option.inspect) { new_job_class { coalesce key: "a", option => :discard } }
    end
  end

  test "on_conflict defaults to discard, with no max_calls" do
    job_class = new_job_class { coalesce key: "a" }

    assert_equal :discard, job_class.coalesce_on_conflict
    assert_nil job_class.coalesce_max_calls
  end

  test "on_conflict is discard or collect, and anything else raises" do
    assert_equal :discard, new_job_class { coalesce key: "a", on_conflict: :discard }.coalesce_on_conflict
    assert_equal :collect, new_job_class { coalesce key: "a", on_conflict: :collect }.coalesce_on_conflict

    [ :replace, :block, "collect", nil ].each do |value|
      error = assert_raises(ArgumentError, value.inspect) { new_job_class { coalesce key: "a", on_conflict: value } }
      assert_equal "`on_conflict` must be :discard or :collect", error.message
    end
  end

  test "max_calls needs on_conflict collect" do
    error = assert_raises(ArgumentError) { new_job_class { coalesce key: "a", max_calls: 5 } }
    assert_equal "`max_calls` needs `on_conflict: :collect`", error.message
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :discard, max_calls: 5 } }
  end

  test "max_calls has to be a positive integer" do
    [ 0, -1, "5", 1.5, false ].each do |value|
      error = assert_raises(ArgumentError, value.inspect) { new_job_class { coalesce key: "a", on_conflict: :collect, max_calls: value } }
      assert_equal "`max_calls` must be a positive integer", error.message
    end

    assert_equal 5, new_job_class { coalesce key: "a", on_conflict: :collect, max_calls: 5 }.coalesce_max_calls
  end

  test "max_calls defaults to 100 when collecting" do
    assert_equal 100, new_job_class { coalesce key: "a", on_conflict: :collect }.coalesce_max_calls
  end

  test "there's no on_full, replace, debounce or max_wait" do
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :collect, on_full: :wait } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :replace } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", debounce: 30.seconds } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :collect, max_wait: 5.minutes } }
  end

  test "collect can't be combined with on_conflict discard, whichever comes first" do
    error = assert_raises(ArgumentError) do
      new_job_class do
        coalesce key: "a", on_conflict: :collect
        limits_concurrency key: "b", on_conflict: :discard
      end
    end
    assert_equal "`coalesce` with `on_conflict: :collect` can't be combined with `limits_concurrency` with `on_conflict: :discard`", error.message

    assert_raises(ArgumentError) do
      new_job_class do
        limits_concurrency key: "b", on_conflict: :discard
        coalesce key: "a", on_conflict: :collect
      end
    end
  end

  test "collect composes with blocking concurrency limits, whichever comes first" do
    assert new_job_class {
      coalesce key: "a", on_conflict: :collect
      limits_concurrency key: "b"
    }

    assert new_job_class {
      limits_concurrency key: "b", on_conflict: :block
      coalesce key: "a", on_conflict: :collect
    }
  end

  test "a recurring task for a collecting job is invalid" do
    task = SolidQueue::RecurringTask.new(key: "collecting", class_name: CollectingRecurringJob.name, schedule: "every hour")

    assert_not task.valid?
    assert_equal [ "Class name can't be a job that uses `coalesce` with `on_conflict: :collect`" ], task.errors.full_messages
  end

  test "configuration validation rejects it too" do
    configuration = SolidQueue::Configuration.new(recurring_schedule_file: config_file_path(:recurring_with_collecting_job))

    assert_not configuration.valid?
    assert_match(/on_conflict: :collect/, configuration.errors.full_messages.to_sentence)
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
