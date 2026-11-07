# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceKeyDeclarationsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class DiscardJob < ApplicationJob
    coalesce key: "default"

    def perform(value)
      JobBuffer.add(value)
    end
  end

  class OtherDiscardJob < DiscardJob
    coalesce key: "default"
  end

  class CollectJob < ApplicationJob
    coalesce key: "default", on_conflict: :collect

    def perform_calls(calls)
      calls.each { |call| JobBuffer.add(call.arguments.first) }
    end
  end

  teardown do
    SolidQueue::Job.destroy_all
    SolidQueue::Batch.destroy_all
  end

  { integer: 123, blank: "", whitespace: " ", empty_symbol: :"", empty_array: [], empty_hash: {}, nil: nil, false: false }.each do |label, key|
    [ false, true ].each do |bulk|
      test "#{label} declaration through #{bulk ? 'bulk' : 'single'} enqueue" do
        previous_key = DiscardJob.coalesce_key
        DiscardJob.coalesce key: key
        jobs = [ DiscardJob.new(0), DiscardJob.new(1) ]

        bulk ? ActiveJob.perform_all_later(jobs) : jobs.each(&:enqueue)

        if key.nil? || key == false
          assert_equal [ nil, nil ], jobs.map(&:coalesce_status)
          assert_equal 2, jobs.map(&:provider_job_id).uniq.size
        else
          assert_equal [ :created, :discarded ], jobs.map(&:coalesce_status)
          assert_equal jobs.first.provider_job_id, jobs.last.provider_job_id
        end

        perform_ready_jobs
        assert_equal (key.nil? || key == false) ? [ 0, 1 ] : [ 0 ], JobBuffer.values
      ensure
        DiscardJob.coalesce_key = previous_key
      end

      test "#{label} declaration with collecting through #{bulk ? 'bulk' : 'single'} enqueue" do
        previous_key = CollectJob.coalesce_key
        CollectJob.coalesce key: key, on_conflict: :collect
        jobs = [ CollectJob.new(0), CollectJob.new(1) ]

        bulk ? ActiveJob.perform_all_later(jobs) : jobs.each(&:enqueue)

        if key.nil? || key == false
          assert_equal [ nil, nil ], jobs.map(&:coalesce_status)
          assert_equal 2, jobs.map(&:provider_job_id).uniq.size
        else
          assert_equal [ :created, :collected ], jobs.map(&:coalesce_status)
          assert_equal jobs.first.provider_job_id, jobs.last.provider_job_id
        end

        perform_ready_jobs
        assert_equal [ 0, 1 ], JobBuffer.values
      ensure
        CollectJob.coalesce_key = previous_key
      end
    end
  end

  test "a blank declaration releases its key when execution starts" do
    previous_key = DiscardJob.coalesce_key
    DiscardJob.coalesce key: ""
    DiscardJob.perform_later(0)

    outcomes = perform_ready_jobs_with_callback { DiscardJob.perform_later(1).coalesce_status }

    assert_equal [ :created ], outcomes
    assert_equal [ 0 ], JobBuffer.values
    perform_ready_jobs
    assert_equal [ 0, 1 ], JobBuffer.values
  ensure
    DiscardJob.coalesce_key = previous_key
  end

  test "an unsupported declaration stays isolated by batch and class" do
    previous_keys = [ DiscardJob, OtherDiscardJob ].map(&:coalesce_key)
    [ DiscardJob, OtherDiscardJob ].each { |job_class| job_class.coalesce key: 123 }
    first = second = nil

    SolidQueue::Batch.enqueue { first = DiscardJob.perform_later(0) }
    SolidQueue::Batch.enqueue { second = DiscardJob.perform_later(1) }
    outside = DiscardJob.perform_later(2)
    other_class = OtherDiscardJob.perform_later(3)

    assert_equal [ :created ] * 4, [ first, second, outside, other_class ].map(&:coalesce_status)
    assert_equal 4, [ first, second, outside, other_class ].map(&:provider_job_id).uniq.size
    perform_ready_jobs
    assert_equal [ 0, 1, 2, 3 ], JobBuffer.values.sort
  ensure
    [ DiscardJob, OtherDiscardJob ].zip(previous_keys).each { |job_class, key| job_class.coalesce_key = key }
  end
end
