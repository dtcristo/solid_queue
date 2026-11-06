class CoalescedResultJob < ApplicationJob
  coalesce key: ->(group) { group }

  def perform(_group)
    JobResult.create!(status: "ran")
  end
end
