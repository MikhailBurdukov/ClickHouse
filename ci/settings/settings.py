S3_BUCKET_NAME = "clickhouse-builds"
S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"

MAIN_BRANCH = "master"

S3_ARTIFACT_PATH = f"{S3_BUCKET_NAME}/artifacts"
CI_CONFIG_RUNS_ON = ["style-checker-aarch64"]
# TODO: cannot crosscompile the image: clickhouse/mysql-java-client. use amd runner to have all images for amd:
DOCKER_BUILD_RUNS_ON = ["style-checker"]
CACHE_S3_PATH = f"{S3_BUCKET_NAME}/ci_ch_cache"
HTML_S3_PATH = f"{S3_BUCKET_NAME}/reports"
S3_BUCKET_TO_HTTP_ENDPOINT = {S3_BUCKET_NAME: S3_BUCKET_HTTP_ENDPOINT}

DOCKERHUB_USERNAME = "robotclickhouse"
DOCKERHUB_SECRET = "dockerhub_robot_password"

CI_DB_DB_NAME = "default"
CI_DB_TABLE_NAME = "checks"

INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS = ""
PIPELINE_PRECHECKS = [
    "python3 ./ci/jobs/scripts/prechecks/pr_description.py",
    "python3 ./ci/jobs/scripts/prechecks/trusted.py",
]
