
import socket
import traceback
from general.logger import node_logger
from general.config import SYNC_CONFIG

class local_socket:
    def __init__(self) -> None:
        self.ip = SYNC_CONFIG["socket_ip"]
        self.port = SYNC_CONFIG["socket_port"]
        self.LOG = node_logger("sync_socket")
        socket.setdefaulttimeout(1)
        self.soc = socket.socket()
        
    def connect(self) -> None:
        try:
            self.soc.connect((self.ip, self.port))
        except Exception:
            error_msg = "socket无法连接,报错为" + traceback.format_exc()
            self.LOG.error(error_msg)
            raise RuntimeError(error_msg)
    
    def send(self, msg: str) -> None:
        try:
            self.soc.send(bytes(msg, encoding='utf-8'))
        except Exception:
            error_msg = "socket发送错误,报错为" + traceback.format_exc()
            self.LOG.error(error_msg)
            raise RuntimeError(error_msg)
        
    def recv(self, buffer_size: int = 1024) -> str:
        try:
            return self.soc.recv(buffer_size).decode("UTF-8")
        except TimeoutError:
            return ""
        except Exception:
            error_msg = "socket接收错误,报错为" + traceback.format_exc()
            self.LOG.error(error_msg)
            raise RuntimeError(error_msg)
            
l_socket = local_socket()