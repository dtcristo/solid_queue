# frozen_string_literal: true

module SolidQueue
  # The Active Job payload of one call collected into a job. It's kept from the
  # moment it's admitted until the job that holds it is destroyed, and a retry moves
  # it to the new job.
  class CoalescedCall < Record
    belongs_to :job

    serialize :arguments, coder: JSON

    # Calls whose job hasn't finished, isn't running and hasn't failed, for measuring the backlog
    scope :waiting, -> { where(job_id: Job.where(finished_at: nil).where.missing(:claimed_execution, :failed_execution).select(:id)) }
  end
end
