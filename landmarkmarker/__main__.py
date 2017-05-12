from pythoncore import Constants, WorkerService
from LandmarkMarker import LandmarkMarker


def handle_task(task_input, task_token):
    ep_id = task_input["epId"]
    hit_id = task_input["hitId"]
    lm = LandmarkMarker(ep_id, hit_id, task_token)
    lm.run()

if __name__ == '__main__':
    thisTask = Constants.TASK_ARNS['LANDMARK_MARKER']

    WorkerService.start(thisTask, handle_task)
