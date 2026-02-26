# Database Cleanup Scripts

These scripts help manage and clean up the Nebula Conduit database.

## Quick Fix (Recommended)

If you're getting `UNIQUE constraint failed` errors when creating pipelines:

```powershell
.\scripts\quick_cleanup.ps1
```

This will:
- Create a backup of your database
- Remove all orphaned components and executions
- Allow you to create new pipelines

## Detailed Cleanup Options

### Show Current Database State

```powershell
.\scripts\cleanup_database.ps1
```

Shows counts of all records in the database.

### Show Orphaned Records

```powershell
.\scripts\cleanup_database.ps1 -ShowOnly
```

Shows how many orphaned records exist without deleting anything.

### Clean Up Orphaned Records

```powershell
.\scripts\cleanup_database.ps1 -CleanOrphaned
```

Removes orphaned records:
- Component executions without components
- Connections without components
- Components without pipelines
- Executions without pipelines

### Delete All Pipeline Data

```powershell
.\scripts\cleanup_database.ps1 -DeleteAll
```

**WARNING**: This deletes ALL pipeline data. You'll be prompted to confirm.

### Custom Database Path

```powershell
.\scripts\cleanup_database.ps1 -DatabasePath "C:\custom\path\db.db" -CleanOrphaned
```

## Manual Cleanup (SQL)

If you prefer to use SQL directly:

```bash
sqlite3 $NEBULA_CONDUIT_HOME/Nebula.Conduit.db < scripts/cleanup_database.sql
```

Or open the database and run queries manually:

```bash
sqlite3 $NEBULA_CONDUIT_HOME/Nebula.Conduit.db
```

## What Changed

The latest version includes:

1. **Foreign Key Constraints Enabled**: SQLite foreign keys are now enabled by default
2. **Explicit Cleanup in Delete**: Pipeline deletion now explicitly removes all related data
3. **Transaction-based Cleanup**: All cleanup operations use transactions for safety

## Troubleshooting

### "sqlite3 command not found"

Download SQLite from: https://www.sqlite.org/download.html

Or use the quick_cleanup.ps1 script which has a fallback method.

### "Database locked"

Stop the Nebula Conduit server before running cleanup scripts.

### "Permission denied"

Run PowerShell as Administrator or check file permissions.

## Backup

All cleanup scripts automatically create backups before making changes:

```
Nebula.Conduit.db.backup_20260226_143052
```

To restore a backup:

```powershell
Copy-Item "Nebula.Conduit.db.backup_20260226_143052" "Nebula.Conduit.db" -Force
```
