from utils.config_util import ConfigUtil
from job.job_factory import JobFactory

def main():
    configs = ConfigUtil.config_with_default()
    JobFactory(configs).create_and_run_job()

if __name__ == "__main__":
    main()
