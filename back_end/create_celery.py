#!/usr/bin/env python3
"""
Script để chạy Celery Worker
Sử dụng: python celery_worker.py
Hoặc: celery -A Task.celery worker --loglevel=info -c 1
"""

import os
import sys
from Task import celery


def main():
    """
    Chạy Celery worker
    """
    print("Starting Celery Worker...")
    print("Available tasks:")
    for task_name in celery.tasks.keys():
        print(f"  - {task_name}")

    # Cấu hình worker
    worker_args = [
        'worker',
        '--loglevel=info',
        '--concurrency=1',  # Số worker processes
        '--prefetch-multiplier=1',  # Số task mỗi worker có thể prefetch
    ]

    # Chạy worker
    celery.worker_main(worker_args)


if __name__ == '__main__':
    main()