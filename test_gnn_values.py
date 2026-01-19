import torch
import json
from src.supply_chain.gnn.model import SupplyChainGNN

with open('models/gnn_scaler.json') as f:
    scaler = json.load(f)

xm = torch.tensor(scaler['x_mean'])
xs = torch.tensor(scaler['x_std'])
em = torch.tensor(scaler['edge_mean'])
es = torch.tensor(scaler['edge_std'])

model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1)
model.load_state_dict(torch.load('models/supply_chain_gnn.pth', map_location='cpu'))
model.eval()

print('=== SCALER MEANS ===')
names = ["Type", "Load", "Traffic", "Weather", "Delay", "Backlog"]
for i in range(6):
    print(f'  {names[i]}: mean={xm[i].item():.2f}, std={xs[i].item():.2f}')

# Create test data
N = 10
src = list(range(9)) + list(range(1,10))
dst = list(range(1,10)) + list(range(9))
edge_index = torch.tensor([src, dst], dtype=torch.long)
edge_attr = torch.zeros(18, 3)
e_norm = (edge_attr - em) / es

print('\n=== TEST 1: IDEAL (all zeros except Type) ===')
x_ideal = torch.zeros(N, 6)
x_ideal[:, 0] = torch.tensor([1,2,3,4,1,2,3,4,1,2])  # Type
x_stacked = torch.cat([x_ideal]*3, dim=1)
x_norm = (x_stacked - xm) / xs

with torch.no_grad():
    logits = model(x_norm, edge_index, e_norm)
    raw = torch.sigmoid(logits)
    # Calibration
    calibrated = torch.clamp((raw - 0.42) * 2.5 + 0.15, 0.0, 1.0)

print(f'Raw:        mean={raw.mean().item():.4f}, max={raw.max().item():.4f}')
print(f'Calibrated: mean={calibrated.mean().item():.4f}, max={calibrated.max().item():.4f}')

print('\n=== TEST 2: SABOTAGED (one node) ===')
x_sab = torch.zeros(N, 6)
x_sab[:, 0] = torch.tensor([1,2,3,4,1,2,3,4,1,2])  # Type
# Sabotage node 0
x_sab[0, 1] = 20    # Load
x_sab[0, 2] = 13.5  # Traffic
x_sab[0, 3] = 1.6   # Weather
x_sab[0, 4] = 120   # Delay
x_sab[0, 5] = 150   # Backlog

x_sab_stacked = torch.cat([x_sab]*3, dim=1)
x_sab_norm = (x_sab_stacked - xm) / xs

with torch.no_grad():
    logits_sab = model(x_sab_norm, edge_index, e_norm)
    raw_sab = torch.sigmoid(logits_sab)
    calibrated_sab = torch.clamp((raw_sab - 0.42) * 2.5 + 0.15, 0.0, 1.0)

print(f'Raw:        mean={raw_sab.mean().item():.4f}, max={raw_sab.max().item():.4f}')
print(f'Calibrated: mean={calibrated_sab.mean().item():.4f}, max={calibrated_sab.max().item():.4f}')
print(f'\nNode 0 (sabotaged):')
print(f'  Raw: {raw_sab[0].item():.4f} -> Calibrated: {calibrated_sab[0].item():.4f}')
print(f'Node 1 (normal):')
print(f'  Raw: {raw_sab[1].item():.4f} -> Calibrated: {calibrated_sab[1].item():.4f}')
