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
        call_dequeue().expect("bank_statements", 1),
        call_size().expect(1),
        call_dequeue().expect("id_verification", 2),
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
    """User with only 2 tasks is NOT promoted ahead of earlier-timestamped user."""
    run_queue([
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(3),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
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
