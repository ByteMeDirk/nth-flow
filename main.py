from nthflow.flow import Flow

flow = Flow("mock_configs/*.yml")
flow.build()

print(flow.flow_dag)
