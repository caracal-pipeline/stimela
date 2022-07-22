try:
    import kubernetes
    AVAILABLE = True
    STATUS = "ok"
    from .run_kube import run
except ImportError:
    AVAILABLE = False
    STATUS = "please reinstall with the optional kube dependency (stimela[kube])"

    def run(*args, **kw):
        raise RuntimeError(f"kubernetes backend {STATUS}")
