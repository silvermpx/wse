// =============================================================================
// WebSocket Engine - Whose data the page's queues hold
// =============================================================================

// A new number each time the page drops one user's data (queue, topics,
// positions) for another's: a message made under an older number is that
// user's, and it does not go out any more.
let epoch = 0;

/** The current owner's number. */
export function ownerEpoch(): number {
  return epoch;
}

/** The data was dropped for another user: from now on a new owner. */
export function nextOwnerEpoch(): number {
  epoch += 1;
  return epoch;
}
