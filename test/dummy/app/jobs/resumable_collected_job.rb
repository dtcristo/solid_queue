begin
  require "active_job/continuation"
rescue LoadError
  # Zeitwerk requires that we define the constant
  class ResumableCollectedJob; end
  return
end

class ResumableCollectedJob < ApplicationJob
  include ActiveJob::Continuable

  coalesce key: "resumable", on_conflict: :collect

  cattr_accessor :stop_after
  self.resume_options = { wait: 0 }

  def perform_calls(calls)
    results = calls.map { |call| call.arguments.first }
    JobBuffer.add([ :run, results.map(&:id) ])

    step :process do |step|
      results.sort_by(&:id).each do |result|
        next if step.cursor && result.id <= step.cursor

        JobBuffer.add([ :processed, result.id ])
        ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = true if result.id == self.class.stop_after
        step.set! result.id
      end
    end
  end
end
