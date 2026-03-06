from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue, call_age, call_purge


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])


# --- R1: Initial tests ---


def test_rule_of_3() -> None:
    """User with 3+ tasks gets promoted ahead of other users."""
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(4),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_timestamp_ordering() -> None:
    """Older timestamp dequeues first when priorities are equal."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_dependency_resolution() -> None:
    """Enqueueing credit_check auto-adds companies_house dependency."""
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])


def test_dequeue_empty_queue() -> None:
    """Dequeue returns None when queue is empty."""
    run_queue([
        {"name": "dequeue", "input": None, "expect": None},
    ])


def test_size_tracking() -> None:
    """Size reflects correct count after enqueues and dequeues."""
    run_queue([
        call_size().expect(0),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("id_verification", 2),
        call_size().expect(1),
        call_dequeue().expect("bank_statements", 1),
        call_size().expect(0),
    ])


def test_purge() -> None:
    """Purge clears the queue and returns True."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(2),
        call_purge().expect(True),
        call_size().expect(0),
    ])


def test_rule_of_3_boundary_no_promotion() -> None:
    """User with only 2 tasks is NOT promoted; bank_statements deprioritized to end."""
    run_queue([
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=4)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=4)).expect(3),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_rule_of_3_two_users_both_promoted() -> None:
    """When both users have 3+ tasks, the one with older earliest timestamp wins."""
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=4)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=4)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=4)).expect(3),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(5),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(6),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_rule_of_3_with_dependencies() -> None:
    """Auto-added dependencies count toward the 3-task threshold."""
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=4)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=4)).expect(3),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(4),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


# --- R2: Deduplication tests ---


def test_dedup_exact_duplicate() -> None:
    """Spec example: same (user_id, provider) enqueued twice, size stays 1."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=4)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=4)).expect(2),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_dedup_keeps_earlier_timestamp() -> None:
    """When existing task has earlier timestamp, it is kept."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=3)).expect(2),
        # user 1's task at t+0 should dequeue before user 2's at t+3
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_dedup_replaces_with_earlier_new_timestamp() -> None:
    """When new task has earlier timestamp, it replaces the existing one."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=3)).expect(2),
        # user 1's task should now have t+0, dequeuing before user 2's t+3
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_dedup_different_users_same_provider() -> None:
    """Same provider for different users are NOT duplicates."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
    ])


def test_dedup_with_dependency_resolution() -> None:
    """Enqueueing credit_check adds companies_house dep; enqueueing companies_house again is a no-op."""
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])


def test_dedup_prevents_false_rule_of_3() -> None:
    """Duplicate enqueues should not inflate task count to trigger Rule of 3."""
    run_queue([
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=4)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=4)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=4)).expect(3),
        # user 1 has 2 unique tasks, not 3 — no promotion
        # id_verification dequeues first (bank_statements deprioritized)
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


# --- R3: Bank statements deprioritization tests ---


def test_r3_spec_example() -> None:
    """R3 spec: bank_statements held back even though it was enqueued first."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=2)).expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r3_bank_statements_after_own_tasks_with_rule_of_3() -> None:
    """R3: promoted user's bank_statements comes after their other tasks."""
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=4)).expect(4),
        # User 1 promoted (3 tasks): companies_house, id_verification, then bank_statements
        # User 2 not promoted, comes after user 1's promoted group
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_r3_multiple_users_bank_statements_no_rule_of_3() -> None:
    """R3: multiple users' bank_statements all go to end, sorted by timestamp."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=4)).expect(2),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=3)).expect(3),
        # Non-bank_statements first, then bank_statements by timestamp
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 3),
    ])


def test_r3_all_bank_statements_falls_through_to_timestamp() -> None:
    """R3 edge case: when all tasks are bank_statements, ordering falls through
    to timestamp as usual."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=5)).expect(3),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 3),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r3_bank_statements_with_dependency_credit_check() -> None:
    """R3: credit_check dependency (companies_house) is not bank_statements,
    so it is not deprioritized; bank_statements goes last within promoted group."""
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),  # adds companies_house
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(4),
        # User 1 has 3 tasks -> promoted to HIGH
        # Within user 1: companies_house, credit_check first, then bank_statements
        # User 2 stays NORMAL
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


# --- R4: Queue internal age tests ---


def test_age_empty_queue() -> None:
    """Age of empty queue is 0."""
    run_queue([
        call_age().expect(0),
    ])


def test_age_single_task() -> None:
    """Age of queue with single task is 0."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_age().expect(0),
    ])


def test_age_multiple_tasks() -> None:
    """Age of queue with multiple tasks is max timestamp - min timestamp."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=10)).expect(3),
        call_age().expect(600),
    ])


def test_age_spec_example() -> None:
    """R4 spec example: two tasks 5 minutes apart -> age is 300 seconds."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        call_age().expect(300),
    ])


def test_age_after_dequeue() -> None:
    """Age updates after dequeueing changes the oldest/newest boundary."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("id_verification", 3, iso_ts(delta_minutes=10)).expect(3),
        call_age().expect(600),
        # Dequeue removes the oldest (user 1 at t+0)
        call_dequeue().expect("id_verification", 1),
        # Age should now be between user 2 (t+5) and user 3 (t+10) = 300s
        call_age().expect(300),
    ])


def test_age_after_purge() -> None:
    """Age returns 0 after purging the queue."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=10)).expect(2),
        call_age().expect(600),
        call_purge().expect(True),
        call_age().expect(0),
    ])


def test_age_same_timestamps() -> None:
    """Age is 0 when all tasks share the same timestamp."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=0)).expect(3),
        call_age().expect(0),
    ])


def test_age_with_dedup_timestamp_replacement() -> None:
    """Dedup replaces with earlier timestamp; age should reflect the updated value."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=10)).expect(2),
        call_age().expect(0),
        # Re-enqueue user 1 with earlier timestamp; dedup replaces it
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(2),
        # Age should now be 600s (t+0 to t+10)
        call_age().expect(600),
    ])


def test_age_with_dependencies() -> None:
    """Auto-added dependency timestamps are included in age calculation."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        # credit_check auto-adds companies_house; both share the t+10 timestamp
        call_enqueue("credit_check", 2, iso_ts(delta_minutes=10)).expect(3),
        # Age spans t+0 (user 1) to t+10 (user 2's tasks) = 600s
        call_age().expect(600),
    ])


# --- R5: Time-sensitive bank statements tests ---


def test_r5_spec_example_1() -> None:
    """R5 spec example 1: bank_statements promoted ahead of companies_house
    but not ahead of id_verification (older timestamp)."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 3),
    ])


def test_r5_spec_example_2_tiebreaker() -> None:
    """R5 spec example 2: FIFO tiebreak between two bank_statements with the
    same timestamp; user 2 was enqueued first so dequeues before user 1."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=2)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=2)).expect(3),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=3)).expect(4),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=10)).expect(5),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("companies_house", 3),
    ])


def test_r5_not_old_enough() -> None:
    """Bank_statements with internal age < 300s stays deprioritized per R3."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=4)).expect(2),
        # Delta 240s < 300s — R5 does not apply, R3 deprioritizes bank_statements
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r5_exactly_300s_boundary() -> None:
    """Bank_statements with exactly 300s internal age qualifies for R5 promotion."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        # Delta exactly 300s — R5 applies; no older tasks, bank_statements first
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_r5_multiple_candidates_different_timestamps() -> None:
    """Multiple qualifying bank_statements: earliest-timestamp candidate wins."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=2)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
        # Both qualify (420s and 300s). User 1 is earliest → dequeues first
        call_dequeue().expect("bank_statements", 1),
        # Re-eval: user 2 still qualifies (300s). No older tasks.
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 3),
    ])


def test_r5_with_rule_of_3_interaction() -> None:
    """Time-sensitive bank_statements for user 2 cuts into user 1's Rule of 3
    group — older-timestamp tasks from the group still dequeue first."""
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=2)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=3)).expect(4),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=10)).expect(5),
        # User 1 has 3 tasks → Rule of 3. bank_statements(2) qualifies for R5.
        # Older tasks block: companies_house(1)@t+0, id_verification(1)@t+1
        call_dequeue().expect("companies_house", 1),
        # User 1 drops to 2 tasks, no longer Rule of 3. id_verification(1) still older.
        call_dequeue().expect("id_verification", 1),
        # No older tasks remain → bank_statements(2) dequeues
        call_dequeue().expect("bank_statements", 2),
        # bank_statements(1) now qualifies (420s), no older tasks
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 3),
    ])


def test_r5_all_bank_statements_some_time_sensitive() -> None:
    """All tasks are bank_statements; only the one with sufficient age gets
    R5 promotion. Remaining tasks sort by timestamp as normal."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=3)).expect(2),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=7)).expect(3),
        # User 1: 420s qualifies. Users 2 & 3 don't.
        call_dequeue().expect("bank_statements", 1),
        # User 2: 240s doesn't qualify. Normal sort by timestamp.
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 3),
    ])


def test_r5_bank_statements_is_oldest_task() -> None:
    """Time-sensitive bank_statements with the oldest timestamp dequeues first,
    overriding R3 deprioritization."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=3)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
        # bank_statements(1): 420s qualifies. No older tasks.
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("companies_house", 3),
    ])


def test_r5_re_evaluation_after_dequeue() -> None:
    """After dequeuing an R5 candidate, the next dequeue re-evaluates.
    A bank_statements that didn't qualify before still doesn't if max_ts
    hasn't changed enough."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=3)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
        # User 1: 420s qualifies. User 2: 240s doesn't.
        call_dequeue().expect("bank_statements", 1),
        # Re-eval: user 2 still only 240s — R3 deprioritizes behind companies_house
        call_dequeue().expect("companies_house", 3),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_r5_no_bank_statements_in_queue() -> None:
    """No bank_statements at all — normal behaviour unchanged."""
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=7)).expect(2),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 2),
    ])

