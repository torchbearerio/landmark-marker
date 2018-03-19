import traceback
from io import BytesIO

from datetime import datetime
from pythoncore import Task, Constants
from pythoncore.AWS import AWSClient
from pythoncore.Model.Hit import Hit
from pythoncore.Model import TorchbearerDB


class PipelineFinisher (Task.Task):

    def __init__(self, ep_id, hit_id, task_token):
        super(PipelineFinisher, self).__init__(ep_id, hit_id, task_token)

    def _run_pipeline_finisher(self):
        print("Starting finisher task for ep {}, hit {}".format(self.ep_id, self.hit_id))

        # Create DB session
        session = TorchbearerDB.Session()

        try:
            hit = session.query(Hit).filter_by(hit_id=self.hit_id).one()

            hit.set_start_time_for_task("pipeline_finish")

            # Mark this hit as "complete"
            hit.status = Constants.HIT_STATUS["HIT_STATUS_COMPLETE"]

            # Update processing end time for hit
            hit.processing_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Get all landmarks for this hit
            landmarks = hit.candidate_landmarks

            # Remove streetview image and saliency map for this ExecutionPoint
            self._delete_s3_objects(Constants.S3_BUCKETS["STREETVIEW_IMAGES"], ["{0}_at.jpg".format(self.ep_id)])
            self._delete_s3_objects(Constants.S3_BUCKETS["STREETVIEW_IMAGES"], ["{0}_before.jpg".format(self.ep_id)])
            self._delete_s3_objects(Constants.S3_BUCKETS["STREETVIEW_IMAGES"], ["{0}_just_before.jpg".format(self.ep_id)])
            self._delete_s3_objects(Constants.S3_BUCKETS["SALIENCY_MAPS"], ["{0}_at.json".format(self.hit_id)])
            self._delete_s3_objects(Constants.S3_BUCKETS["SALIENCY_MAPS"], ["{0}_before.json".format(self.hit_id)])
            self._delete_s3_objects(Constants.S3_BUCKETS["SALIENCY_MAPS"], ["{0}_just_before.json".format(self.hit_id)])

            # Delete any images associated with this landmark
            landmark_keys = map(lambda lm: "{0}.png".format(lm.landmark_id), landmarks)
            self._delete_s3_objects(Constants.S3_BUCKETS["CROPPED_IMAGES"], landmark_keys)
            self._delete_s3_objects(Constants.S3_BUCKETS["TRANSPARENT_CROPPED_IMAGES"], landmark_keys)
            self._delete_s3_objects(Constants.S3_BUCKETS["MARKED_LANDMARK_IMAGES"], landmark_keys)

            hit.set_end_time_for_task("pipeline_finish")

            session.commit()
            self.send_success()

            print("Completed finisher task for ep {}, hit {}".format(self.ep_id, self.hit_id))

        except Exception, e:
            traceback.print_exc()
            session.rollback()
            self.send_failure('PIPELINE FINISHER ERROR', e.message)

        finally:
            session.close()

    @staticmethod
    def _delete_s3_objects(bucket, keys):
        client = AWSClient.get_client('s3')
        key_dict = dict(('Key', keyName) for keyName in keys)
        try:
            client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': [
                        key_dict
                    ]
                }
            )
        except Exception:
            print "Couldn't delete the given S3 object."

    def run(self):
        self._run_pipeline_finisher()

if __name__ == '__main__':
    sn = PipelineFinisher(1, 6, "qwd")
    sn.run()
