import gymnasium as gym
from gymnasium import spaces
import numpy as np
import torch as th
from stable_baselines3 import DQN

class KrateEnv(gym.Env):
    """Simulates Krate's asynchronous gossip protocol and traffic patterns."""
    def __init__(self):
        super(KrateEnv, self).__init__()
        # Actions: 0 = Peer 1, 1 = Peer 2, 2 = Redis
        self.action_space = spaces.Discrete(3)
        # State: [gossip_surplus, delta_t, velocity, local_load, redis_lat]
        self.observation_space = spaces.Box(low=0, high=1, shape=(5,), dtype=np.float32)
        
        self.true_surplus = 1.0
        self.gossip_surplus = 1.0
        self.delta_t = 0.0

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        self.true_surplus = 1.0
        self.gossip_surplus = 1.0
        self.delta_t = 0.0
        return np.array([1.0, 0.0, 0.5, 0.5, 0.2], dtype=np.float32), {}

    def step(self, action):
        # 1. Simulate background token consumption (traffic burstiness)
        consumption = np.random.uniform(0.05, 0.2)
        self.true_surplus = max(0.0, self.true_surplus - consumption)
        
        # 2. Evaluate the Agent's Action
        reward = 0
        terminated = False
        
        if action == 2:
            # Routed to Redis (Safe but slow)
            reward = -2
        else:
            # Routed to a Peer based on stale state
            if self.true_surplus > 0:
                reward = 1  # Success! Tokens were actually available.
            else:
                reward = -100 # Catastrophic timeout! Stale probe hit exhausted peer.
                terminated = True
                
        # 3. Simulate Gossip Asynchrony (10% chance of receiving a heartbeat this tick)
        if np.random.rand() < 0.10:
            self.gossip_surplus = self.true_surplus
            self.delta_t = 0.0 # Reset staleness timer
        else:
            self.delta_t = min(1.0, self.delta_t + 0.1) # Timer increases

        # 4. Construct the next Stale Observation for the agent
        next_state = np.array([
            self.gossip_surplus, 
            self.delta_t, 
            0.5, # Mock velocity
            0.5, # Mock load
            0.2  # Mock redis latency
        ], dtype=np.float32)
        
        return next_state, reward, terminated, False, {}

if __name__ == "__main__":
    print("Initializing Krate cluster simulation...")
    env = KrateEnv()

    # Define a tiny neural network architecture for microsecond inference
    # 2 hidden layers of 32 nodes each.
    policy_kwargs = dict(net_arch=[32, 32])

    print("Training DQN Agent (this will take a few seconds)...")
    model = DQN(
        "MlpPolicy", 
        env, 
        policy_kwargs=policy_kwargs, 
        learning_rate=1e-3, 
        buffer_size=10000, 
        learning_starts=1000, 
        batch_size=64,
        gamma=0.99, # Discount factor
        verbose=0
    )
    
    # Train for 20,000 requests
    model.learn(total_timesteps=2000000)
    
    print("Training complete! Extracting PyTorch Q-Network...")
    
    # Extract the underlying PyTorch network from Stable-Baselines3
    q_network = model.policy.q_net
    q_network.eval()
    
    # Export to ONNX
    dummy_input = th.randn(1, 5)
    th.onnx.export(
        q_network,
        dummy_input,
        "../krate_dqn.onnx", # Save directly to the repo root
        export_params=True,
        input_names=['state_input'],
        output_names=['q_values']
    )
    print("Successfully exported krate_dqn.onnx to the root directory.")