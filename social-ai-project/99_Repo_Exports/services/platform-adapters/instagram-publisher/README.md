# instagram-publisher (Phase 9+ — shell)

Instagram stays a **manual-export shell** behind an interface boundary (plan §15.3),
because Meta's publishing API was not verified. This adapter never autoposts.

`InstagramPublisher.draft(job)` prepares asset+caption, pushes the job to the
`social:instagram:manual_export` Redis list, and returns a draft placeholder
(`status=manual_export_pending`). The operator finishes posting in the Instagram app.

Real Graph API publishing is a separate verification workstream — implement live mode
only after Meta endpoints/permissions are confirmed, then add MetricsPort/DraftPort.

Consumes `social:publish:requests` (platform=instagram) → `social:publish:status`.
