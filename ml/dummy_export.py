import torch
import torch.nn as nn
class DummyRouterNet(nn.Module):
    def __init__(self, input_size=5, num_actions=3):
        super().__init__()
        self.fc1=nn.Linear(input_size,32)
        self.relu=nn.ReLU()
        self.fc2=nn.Linear(32,num_actions)
    def forward(self,x):
        return self.fc2(self.relu(self.fc1(x)))
model= DummyRouterNet()
model.eval()
dummy_state=torch.randn(1,5)
torch.onnx.export(model,
                  dummy_state,
                  "dummy_router.onnx",
                  export_params=True,
                  input_names=['state_input'],
                  output_names=['q_values'],
                  dynamic_axes={'state_input':{0:'batch_size'},'q_values':{0:'batch_size'}}
                )
print("dummy_router.onnx generated successfully in the root directory")
