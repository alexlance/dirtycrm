pg_dump \
  --no-password \
  --format=p \
  --blobs \
  --verbose \
  --create \
  --clean \
  --if-exists \
  --column-inserts \
  --encoding=UTF8 \
  -f backup-$(date +%F).sql \
  -U $DIRTY_USER \
  -h $DIRTY_HOST \
  -p $DIRTY_PORT \
  $DIRTY_DB

