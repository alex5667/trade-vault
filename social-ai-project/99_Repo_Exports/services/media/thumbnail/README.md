# media-thumbnail (Phase 3, §6.2)

Selects the best frame as a thumbnail candidate. `social:media:frames` →
`social:media:thumbnail` (`media_thumbnail.v1`). Thumbnail/title matter most on YouTube
(plan §15.1); the chosen frame feeds the YouTube publisher + thumbnail A/B later.

Deterministic scorer (`score_frame`) — golden-testable; swap for an aesthetic/saliency
model when available. No external deps.
