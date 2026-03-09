bind = "0.0.0.0:5045"

workers = 1
worker_class = "gthread"
threads = 4

max_requests = 0
max_requests_jitter = 0

timeout = 120
graceful_timeout = 30
keepalive = 75

loglevel = "info"
errorlog = "-"
accesslog = None

reload = False
