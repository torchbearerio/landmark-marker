from pythoncore import Constants, WorkerService
from PipelineFinisher import PipelineFinisher


def handle_task(task_input, task_token):
    ep_id = task_input["epId"]
    hit_id = task_input["hitId"]
    pf = PipelineFinisher(ep_id, hit_id, task_token)
    pf.run()

if __name__ == '__main__':
    # PipelineFinisher(358, 812, "fsdf").run()

    thisTask = Constants.TASK_ARNS['PIPELINE_FINISH']

    WorkerService.start((thisTask, handle_task))
