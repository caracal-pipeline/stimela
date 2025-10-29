from dataclasses import dataclass
from typing import Optional

# NOTE(JSKenyon): At present, this is still part of kube_utils.py. Evenutally, it would be nice
# to move this functionality here so that we can manage it more generally.


@dataclass
class KubeReport:
    status: Optional[str] = None
    running_pods: Optional[int] = None
    pending_pods: Optional[int] = None
    terminating_pods: Optional[int] = None
    successful_pods: Optional[int] = None
    failed_pods: Optional[int] = None
    stateless_pods: Optional[int] = None
    total_pods: Optional[int] = None
    total_cores: Optional[float] = None
    total_memory: Optional[float] = None
    connection_status: str = "connected"

    @property
    def profiling_results(self):
        return {"k8s_cores": self.total_cores, "k8s_mem": self.total_memory}
