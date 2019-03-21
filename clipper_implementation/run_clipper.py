from clipper_admin import ClipperConnection, DockerContainerManager
import p1_agg_clipper

clipper_conn = ClipperConnection(DockerContainerManager())

clipper_conn.start_clipper()

clipper_conn.register_application(name="p1app", input_type="strings", 
                                  default_output="-1.0", slo_micros=100000)

clipper_conn.get_all_apps()

model_r = p1_agg_clipper.main()

def feature_sum(xs):
    forecast_df = p1_agg_clipper.main()
    return [str(x) for x in forecast_df.yhat]
    #return [str(sum(x)) for x in xs]   

def predict(delay):
    #delay = eval(delay)
    future_r = model_r.make_future_dataframe(periods=30,freq='D')
    forecast_r = model_r.predict(future_r)
    #forecast_r.index = forecast_r['ds']
    #forecast 
    #pred_r = pd.DataFrame(forecast_r['yhat'][len(forecast_r)-delay:len(forecast_r)])
    #pred_r=pred_r.reset_index()
    #pred_r = pred_r.to_json()
    return forecast_r.to_json()

from clipper_admin.deployers import python as python_deployer

python_deployer.deploy_python_closure(clipper_conn, name="p1model", version=1, 
                                      input_type="strings", func=predict, 
                                      pkgs_to_install=['pandas','fbprophet==0.4'])

clipper_conn.link_model_to_app(app_name="p1app", model_name="p1model")

#import requests, json, numpy as np
#headers = {"Content-type": "application/json"}
#datas = json.dumps({"input": list(np.random.random(10))})

#requests.post("http://10.65.47.80:1337/p1app/predict", headers=headers, data=datas).json()

