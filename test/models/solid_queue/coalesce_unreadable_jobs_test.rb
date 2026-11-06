# frozen_string_literal: true

require "test_helper"

# A job whose payload or class can't be loaded has to fail and discard the way an
# ordinary job does, so a worker never leaves it claimed, and it has to give up its key.
class SolidQueue::CoalesceUnreadableJobsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class CollectingJob < ApplicationJob
    coalesce key: "unreadable", on_conflict: :collect

    def perform_calls(_calls); end
  end

  class DiscardingJob < ApplicationJob
    coalesce key: "unreadable"

    def perform; end
  end

  PAYLOADS = { "malformed JSON" => "{not json", "JSON null" => "null", "a JSON array" => "[1]", "a JSON string" => "\"x\"" }

  BROKEN_CLASS_FILES = {
    "BrokenSyntaxJob" => "class BrokenSyntaxJob <",
    "BrokenDependencyJob" => "class BrokenDependencyJob < SomeMissingDependency; end",
    "BrokenRequireJob" => "require \"a/file/that/does/not/exist\"",
    "BrokenRaisingJob" => "raise \"can't load\""
  }

  setup do
    @broken_class_dir = Dir.mktmpdir
    BROKEN_CLASS_FILES.each do |name, source|
      path = File.join(@broken_class_dir, "#{name.underscore}.rb")
      File.write(path, source)
      Object.autoload(name.to_sym, path)
    end
  end

  teardown do
    BROKEN_CLASS_FILES.each_key { |name| Object.send(:remove_const, name) if Object.const_defined?(name, false) }
    FileUtils.remove_entry(@broken_class_dir)
  end

  PAYLOADS.each do |description, payload|
    test "an ordinary job with #{description} for a payload fails instead of staying claimed" do
      job = enqueue(AddToBufferJob, "a")
      corrupt(job, payload)

      assert_fails_and_stops_being_claimed(job)
    end

    test "an ordinary job with #{description} for a payload gets discarded" do
      job = enqueue(AddToBufferJob, "a")
      corrupt(job, payload)

      assert_discards(job)
    end

    [ CollectingJob, DiscardingJob ].each do |job_class|
      test "a #{job_class.name.demodulize} with #{description} for a payload fails and gives up its key" do
        job = enqueue(job_class)
        corrupt(job, payload)

        assert_fails_and_stops_being_claimed(job)
        assert_equal 0, SolidQueue::CoalesceKey.count
        assert_equal CollectingJob == job_class ? 1 : 0, SolidQueue::CoalescedCall.where(job_id: job.id).count
      end

      test "a #{job_class.name.demodulize} with #{description} for a payload gets discarded with its key" do
        job = enqueue(job_class)
        corrupt(job, payload)

        assert_discards(job)
        assert_equal 0, SolidQueue::CoalesceKey.count
        assert_equal 0, SolidQueue::CoalescedCall.count
      end
    end
  end

  BROKEN_CLASS_FILES.each_key do |class_name|
    test "an ordinary job whose class is in a file that won't load, #{class_name}, fails instead of staying claimed" do
      job = enqueue(AddToBufferJob, "a")
      rename_job_class(job, class_name)

      assert_fails_and_stops_being_claimed(SolidQueue::Job.find(job.id))
    end

    test "an ordinary job whose class is in a file that won't load, #{class_name}, gets discarded" do
      job = enqueue(AddToBufferJob, "a")
      rename_job_class(job, class_name)

      assert_discards(SolidQueue::Job.find(job.id))
    end

    [ CollectingJob, DiscardingJob ].each do |job_class|
      test "a #{job_class.name.demodulize} whose class is in a file that won't load, #{class_name}, fails and gives up its key" do
        job = enqueue(job_class)
        rename_job_class(job, class_name)

        assert_fails_and_stops_being_claimed(SolidQueue::Job.find(job.id))
        assert_equal 0, SolidQueue::CoalesceKey.count
      end

      test "an unmarked #{job_class.name.demodulize} whose class is in a file that won't load, #{class_name}, gets discarded with its key" do
        job = enqueue(job_class)
        job.update!(class_name: class_name, arguments: job.arguments.except("solid_queue_coalesce_on_conflict").merge("job_class" => class_name))

        assert_discards(SolidQueue::Job.find(job.id))
        assert_equal 0, SolidQueue::CoalesceKey.count
      end
    end
  end

  [ CollectingJob, DiscardingJob ].each do |job_class|
    test "a #{job_class.name.demodulize} whose class is gone fails and gives up its key" do
      job = enqueue(job_class)
      rename_job_class(job, "RemovedJob")

      assert_fails_and_stops_being_claimed(SolidQueue::Job.find(job.id))
      assert_equal 0, SolidQueue::CoalesceKey.count
    end

    test "a #{job_class.name.demodulize} whose class is gone gets discarded with its key" do
      job = enqueue(job_class)
      rename_job_class(job, "RemovedJob")

      assert_discards(SolidQueue::Job.find(job.id))
      assert_equal 0, SolidQueue::CoalesceKey.count
      assert_equal 0, SolidQueue::CoalescedCall.count
    end
  end

  test "a job whose class is gone and has no key fails and gets discarded" do
    job = enqueue(AddToBufferJob, "a")
    rename_job_class(job, "RemovedJob")
    assert_fails_and_stops_being_claimed(SolidQueue::Job.find(job.id))

    assert_discards(SolidQueue::Job.find(job.id))
  end

  private
    def enqueue(job_class, *arguments)
      SolidQueue::Job.find(job_class.perform_later(*arguments).provider_job_id)
    end

    # Written around the model, which can't serialize some of these
    def corrupt(job, payload)
      SolidQueue::Job.where(id: job.id).update_all(arguments: payload)
    end

    def assert_fails_and_stops_being_claimed(job)
      assert_raises(Exception) { perform_ready_jobs }

      assert_not SolidQueue::ClaimedExecution.exists?(job_id: job.id), "left claimed"
      assert SolidQueue::FailedExecution.exists?(job_id: job.id), "didn't fail"
    end

    def assert_discards(job)
      SolidQueue::Job.find(job.id).discard

      assert_not SolidQueue::Job.exists?(job.id), "wasn't discarded"
      assert_not SolidQueue::ReadyExecution.exists?(job_id: job.id), "left ready"
      assert_not SolidQueue::FailedExecution.exists?(job_id: job.id), "left failed"
    end
end
