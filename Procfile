# Procfile
web: gunicorn main:app \
   --workers 1 \
   --worker-class uvicorn.workers.UvicornWorker \
   --bind 0.0.0.0:8443 \
   --timeout 600