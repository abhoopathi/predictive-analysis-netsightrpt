from clipper_admin import ClipperConnection, DockerContainerManager
import p1_agg_clipper

clipper_conn = ClipperConnection(DockerContainerManager())

clipper_conn.start_clipper()

clipper_conn.register_application(name="p1app", input_type="doubles", 
                                  default_output="-1.0", slo_micros=100000)

clipper_conn.get_all_apps()

def feature_sum(xs):
    forecast_df = p1_agg_clipper.main()
    return [str(x) for x in forecast_df.yhat]
    #return [str(sum(x)) for x in xs]

from clipper_admin.deployers import python as python_deployer

python_deployer.deploy_python_closure(clipper_conn, name="p1model", version=1, 
                                      input_type="doubles", func=feature_sum)

clipper_conn.link_model_to_app(app_name="p1app", model_name="p1model")

#import requests, json, numpy as np
#headers = {"Content-type": "application/json"}
#datas = json.dumps({"input": list(np.random.random(10))})

#requests.post("http://10.65.47.80:1337/p1app/predict", headers=headers, data=datas).json()

