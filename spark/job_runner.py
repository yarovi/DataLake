import json
import subprocess
import threading

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

SPARK_SCRIPT = (
    "/opt/spark/northwind-jobs/silver_to_gold_v6.py"
)

SPARK_MASTER = "spark://spark-master:7077"

job_lock = threading.Lock()


class JobHandler(BaseHTTPRequestHandler):

    def respond(self, status, payload):
        body = json.dumps(payload).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

        self.wfile.write(body)

    def do_GET(self):
        if self.path != "/health":
            self.respond(404, {"error": "Not found"})
            return

        self.respond(200, {
            "status": "UP",
            "service": "northwind-spark-job-runner"
        })

    def do_POST(self):
        if self.path != "/jobs/silver-to-gold":
            self.respond(404, {"error": "Not found"})
            return

        if not job_lock.acquire(blocking=False):
            self.respond(409, {
                "status": "BUSY",
                "message": "Ya existe una ejecución Spark en curso"
            })
            return

        try:
            command = [
                SPARK_SUBMIT,
                "--master", SPARK_MASTER,
                "--deploy-mode", "client",
                "--conf", "spark.driver.host=spark-job-runner",
                "--conf", "spark.driver.bindAddress=0.0.0.0",
                SPARK_SCRIPT
            ]

            print("[SPARK-RUNNER] Iniciando Silver → Gold", flush=True)

            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=900
            )

            print(result.stdout, flush=True)

            if result.returncode != 0:
                self.respond(500, {
                    "status": "FAILED",
                    "exit_code": result.returncode,
                    "log_tail": result.stdout[-8000:]
                })
                return

            self.respond(200, {
                "status": "SUCCESS",
                "exit_code": 0,
                "message": "Silver → Gold finalizado"
            })

        except subprocess.TimeoutExpired:
            self.respond(504, {
                "status": "TIMEOUT",
                "message": "Spark excedió los 900 segundos"
            })

        except Exception as error:
            self.respond(500, {
                "status": "FAILED",
                "message": str(error)
            })

        finally:
            job_lock.release()


if __name__ == "__main__":
    server = ThreadingHTTPServer(
        ("0.0.0.0", 8090),
        JobHandler
    )

    print("[SPARK-RUNNER] Escuchando en puerto 8090", flush=True)

    server.serve_forever()