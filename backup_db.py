#!/usr/bin/env python3
import os
import shutil
import sqlite3
from datetime import datetime, timedelta

DEFAULT_DB = "tasks.db"
DEFAULT_BACKUP_DIR = "backups"


def backup_db(db_path: str = DEFAULT_DB, backup_dir: str = DEFAULT_BACKUP_DIR):
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return
    
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"tasks_{timestamp}.db")
    
    shutil.copy2(db_path, backup_path)
    print(f"Backed up: {backup_path}")
    
    cleanup_old_backups(backup_dir, keep_days=7)


def cleanup_old_backups(backup_dir: str, keep_days: int = 7):
    cutoff = datetime.now() - timedelta(days=keep_days)
    
    for f in os.listdir(backup_dir):
        if not f.startswith("tasks_") or not f.endswith(".db"):
            continue
        
        fpath = os.path.join(backup_dir, f)
        mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
        
        if mtime < cutoff:
            os.remove(fpath)
            print(f"Deleted old backup: {f}")


def restore_db(backup_file: str, target_db: str = DEFAULT_DB):
    if not os.path.exists(backup_file):
        print(f"Backup not found: {backup_file}")
        return
    
    if os.path.exists(target_db):
        backup_target = f"{target_db}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
        shutil.copy2(target_db, backup_target)
        print(f"Backed up current DB to: {backup_target}")
    
    shutil.copy2(backup_file, target_db)
    print(f"Restored: {target_db}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Backup/restore SQLite database")
    parser.add_argument("--db", default=DEFAULT_DB, help="Database to backup")
    parser.add_argument("--backup-dir", default=DEFAULT_BACKUP_DIR, help="Backup directory")
    parser.add_argument("--restore", help="Restore from backup file")
    parser.add_argument("--keep-days", type=int, default=7, help="Days to keep backups")
    
    args = parser.parse_args()
    
    if args.restore:
        restore_db(args.restore, args.db)
    else:
        backup_db(args.db, args.backup_dir)


if __name__ == "__main__":
    main()