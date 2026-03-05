from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])


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
        {"name": "purge", "input": None, "expect": True},
        call_size().expect(0),
    ])


def test_rule_of_3_boundary_no_promotion() -> None:
    """User with only 2 tasks is NOT promoted; bank_statements deprioritized to end."""
    run_queue([
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(3),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_rule_of_3_two_users_both_promoted() -> None:
    """When both users have 3+ tasks, the one with older earliest timestamp wins."""
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(3),
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
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(3),
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
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
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
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(3),
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
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(4),
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
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
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




