#!/usr/bin/env python3
"""
Train Entry and Exit ML models for momentum trading.

Entry Model: Predicts P(momentum spike in next 10s)
Exit Model: Predicts P(price continues in current direction for 30s)

Both models use tick-level features and the conditional probability framework.
"""

import os
import glob
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_recall_curve, f1_score
import matplotlib.pyplot as plt

# ============================================================================
# CONFIGURATION
# ============================================================================

TRAINING_DATA_DIR = "./training_data"
MODELS_DIR = "./models"
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Features used for training
ENTRY_FEATURES = [
    'z_score',
    'remaining_pct', 
    'true_prob_up',
    'price_change_500ms',
    'volume_imbalance_500ms',
    'tick_rate_500ms',
    'consecutive_buys_500ms',
    'consecutive_sells_500ms',
    'price_change_1s',
    'volume_imbalance_1s',
    'tick_rate_1s',
    'price_change_5s',
    'volume_imbalance_5s',
    'realized_vol_1min',
    'vol_of_vol',
]

EXIT_FEATURES = ENTRY_FEATURES  # Same features for now

# ============================================================================
# DATASET
# ============================================================================

class MomentumDataset(Dataset):
    def __init__(self, features: np.ndarray, labels: np.ndarray):
        self.features = torch.FloatTensor(features)
        self.labels = torch.FloatTensor(labels)
    
    def __len__(self):
        return len(self.labels)
    
    def __getitem__(self, idx):
        return self.features[idx], self.labels[idx]

# ============================================================================
# MODEL ARCHITECTURE
# ============================================================================

class MomentumMLP(nn.Module):
    """
    Simple MLP for momentum prediction.
    Architecture matches what we can easily export to plain text for Rust.
    """
    def __init__(self, input_dim: int, hidden_dims: list = [64, 32, 16]):
        super().__init__()
        
        layers = []
        prev_dim = input_dim
        
        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_dim),
                nn.Dropout(0.2),
            ])
            prev_dim = hidden_dim
        
        layers.append(nn.Linear(prev_dim, 1))
        layers.append(nn.Sigmoid())
        
        self.net = nn.Sequential(*layers)
    
    def forward(self, x):
        return self.net(x).squeeze(-1)

class MomentumMLPSimple(nn.Module):
    """
    Simpler MLP without BatchNorm for easier Rust export.
    """
    def __init__(self, input_dim: int, h1: int = 64, h2: int = 32, h3: int = 16):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, h1)
        self.fc2 = nn.Linear(h1, h2)
        self.fc3 = nn.Linear(h2, h3)
        self.fc4 = nn.Linear(h3, 1)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.relu(self.fc3(x))
        x = self.sigmoid(self.fc4(x))
        return x.squeeze(-1)

# ============================================================================
# TRAINING
# ============================================================================

def load_data(label_column: str) -> tuple:
    """Load and prepare training data"""
    # Find all labeled parquet files
    files = glob.glob(f"{TRAINING_DATA_DIR}/*_labeled.parquet")
    
    if not files:
        raise ValueError(f"No labeled data found in {TRAINING_DATA_DIR}")
    
    print(f"[*] Loading {len(files)} data files...")
    
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    
    print(f"[*] Total samples: {len(df)}")
    
    # Filter to rows with valid labels
    df = df.dropna(subset=[label_column])
    print(f"[*] Samples with {label_column}: {len(df)}")
    
    # Get features and labels
    X = df[ENTRY_FEATURES].values
    y = df[label_column].values.astype(float)
    
    # Handle imbalanced classes
    pos_rate = y.mean()
    print(f"[*] Positive rate: {pos_rate:.2%}")
    
    return X, y

def train_model(
    model: nn.Module,
    train_loader: DataLoader,
    val_loader: DataLoader,
    epochs: int = 100,
    lr: float = 0.001,
    patience: int = 10,
) -> dict:
    """Train model with early stopping"""
    
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.BCELoss()
    
    model.to(DEVICE)
    
    best_val_auc = 0
    best_state = None
    patience_counter = 0
    history = {'train_loss': [], 'val_loss': [], 'val_auc': []}
    
    for epoch in range(epochs):
        # Training
        model.train()
        train_loss = 0
        for X_batch, y_batch in train_loader:
            X_batch, y_batch = X_batch.to(DEVICE), y_batch.to(DEVICE)
            
            optimizer.zero_grad()
            pred = model(X_batch)
            loss = criterion(pred, y_batch)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item()
        
        train_loss /= len(train_loader)
        
        # Validation
        model.eval()
        val_loss = 0
        all_preds = []
        all_labels = []
        
        with torch.no_grad():
            for X_batch, y_batch in val_loader:
                X_batch, y_batch = X_batch.to(DEVICE), y_batch.to(DEVICE)
                pred = model(X_batch)
                loss = criterion(pred, y_batch)
                val_loss += loss.item()
                all_preds.extend(pred.cpu().numpy())
                all_labels.extend(y_batch.cpu().numpy())
        
        val_loss /= len(val_loader)
        val_auc = roc_auc_score(all_labels, all_preds)
        
        history['train_loss'].append(train_loss)
        history['val_loss'].append(val_loss)
        history['val_auc'].append(val_auc)
        
        if epoch % 10 == 0:
            print(f"  Epoch {epoch}: train_loss={train_loss:.4f}, val_loss={val_loss:.4f}, val_auc={val_auc:.4f}")
        
        # Early stopping
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_state = model.state_dict().copy()
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print(f"  Early stopping at epoch {epoch}")
                break
    
    # Restore best model
    model.load_state_dict(best_state)
    
    return history

def export_model_to_txt(model: MomentumMLPSimple, scaler: StandardScaler, path: str):
    """
    Export model to plain text format for Rust.
    Format matches MLPredictor::load() in ml_bot_tui.rs
    """
    state = model.state_dict()
    
    # Get layer dims
    fc1_w = state['fc1.weight'].cpu().numpy()  # (h1, input)
    fc1_b = state['fc1.bias'].cpu().numpy()    # (h1,)
    fc2_w = state['fc2.weight'].cpu().numpy()  # (h2, h1)
    fc2_b = state['fc2.bias'].cpu().numpy()    # (h2,)
    fc3_w = state['fc3.weight'].cpu().numpy()  # (h3, h2)
    fc3_b = state['fc3.bias'].cpu().numpy()    # (h3,)
    fc4_w = state['fc4.weight'].cpu().numpy()  # (1, h3)
    fc4_b = state['fc4.bias'].cpu().numpy()    # (1,)
    
    input_dim = fc1_w.shape[1]
    h1 = fc1_w.shape[0]
    h2 = fc2_w.shape[0]
    h3 = fc3_w.shape[0]
    
    with open(path, 'w') as f:
        # Header: input h1 h2 h3 1
        f.write(f"{input_dim} {h1} {h2} {h3} 1\n")
        
        # Layer 1 weights (h1 x input)
        for row in fc1_w:
            f.write(' '.join(f"{v:.8f}" for v in row) + '\n')
        # Layer 1 bias
        f.write(' '.join(f"{v:.8f}" for v in fc1_b) + '\n')
        
        # Layer 2 weights (h2 x h1)
        for row in fc2_w:
            f.write(' '.join(f"{v:.8f}" for v in row) + '\n')
        # Layer 2 bias
        f.write(' '.join(f"{v:.8f}" for v in fc2_b) + '\n')
        
        # Layer 3 weights (h3 x h2)
        for row in fc3_w:
            f.write(' '.join(f"{v:.8f}" for v in row) + '\n')
        # Layer 3 bias
        f.write(' '.join(f"{v:.8f}" for v in fc3_b) + '\n')
        
        # Layer 4 weights (1 x h3)
        for row in fc4_w:
            f.write(' '.join(f"{v:.8f}" for v in row) + '\n')
        # Layer 4 bias
        f.write(' '.join(f"{v:.8f}" for v in fc4_b) + '\n')
    
    print(f"[OK] Saved model to {path}")
    
    # Save normalization params
    norm_path = path.replace('.txt', '_norm.txt')
    with open(norm_path, 'w') as f:
        f.write(' '.join(f"{v:.8f}" for v in scaler.mean_) + '\n')
        f.write(' '.join(f"{v:.8f}" for v in scaler.scale_) + '\n')
    
    print(f"[OK] Saved normalization to {norm_path}")

# ============================================================================
# MAIN
# ============================================================================

def train_entry_model():
    """Train the entry model (predicts momentum spikes)"""
    print("\n" + "="*60)
    print("TRAINING ENTRY MODEL")
    print("="*60)
    
    # Load data - use spike_up OR spike_down as target
    X, y_up = load_data('spike_up_10s')
    _, y_down = load_data('spike_down_10s')
    
    # Combined target: any spike
    y = np.logical_or(y_up, y_down).astype(float)
    print(f"[*] Any spike rate: {y.mean():.2%}")
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Split
    X_train, X_val, y_train, y_val = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Create dataloaders
    train_ds = MomentumDataset(X_train, y_train)
    val_ds = MomentumDataset(X_val, y_val)
    
    train_loader = DataLoader(train_ds, batch_size=256, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=256, shuffle=False)
    
    # Train
    model = MomentumMLPSimple(len(ENTRY_FEATURES), h1=64, h2=32, h3=16)
    print(f"[*] Model: {model}")
    
    history = train_model(model, train_loader, val_loader, epochs=100, lr=0.001)
    
    # Evaluate
    model.eval()
    with torch.no_grad():
        X_val_t = torch.FloatTensor(X_val).to(DEVICE)
        preds = model(X_val_t).cpu().numpy()
    
    auc = roc_auc_score(y_val, preds)
    print(f"\n[*] Final validation AUC: {auc:.4f}")
    
    # Find optimal threshold
    precision, recall, thresholds = precision_recall_curve(y_val, preds)
    f1_scores = 2 * precision * recall / (precision + recall + 1e-8)
    best_idx = np.argmax(f1_scores)
    best_threshold = thresholds[best_idx]
    best_f1 = f1_scores[best_idx]
    
    print(f"[*] Best threshold: {best_threshold:.4f} (F1={best_f1:.4f})")
    
    # Export
    os.makedirs(MODELS_DIR, exist_ok=True)
    export_model_to_txt(model, scaler, f"{MODELS_DIR}/entry_model.txt")
    
    # Save threshold
    with open(f"{MODELS_DIR}/entry_threshold.txt", 'w') as f:
        f.write(f"{best_threshold:.8f}\n")
    
    return model, scaler, history

def train_exit_model():
    """Train the exit model (predicts continuation)"""
    print("\n" + "="*60)
    print("TRAINING EXIT MODEL")
    print("="*60)
    
    # Load data
    X, y = load_data('continuation_30s')
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Split
    X_train, X_val, y_train, y_val = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Create dataloaders
    train_ds = MomentumDataset(X_train, y_train)
    val_ds = MomentumDataset(X_val, y_val)
    
    train_loader = DataLoader(train_ds, batch_size=256, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=256, shuffle=False)
    
    # Train
    model = MomentumMLPSimple(len(EXIT_FEATURES), h1=64, h2=32, h3=16)
    print(f"[*] Model: {model}")
    
    history = train_model(model, train_loader, val_loader, epochs=100, lr=0.001)
    
    # Evaluate
    model.eval()
    with torch.no_grad():
        X_val_t = torch.FloatTensor(X_val).to(DEVICE)
        preds = model(X_val_t).cpu().numpy()
    
    auc = roc_auc_score(y_val, preds)
    print(f"\n[*] Final validation AUC: {auc:.4f}")
    
    # Export
    export_model_to_txt(model, scaler, f"{MODELS_DIR}/exit_model.txt")
    
    return model, scaler, history

def plot_history(history: dict, name: str):
    """Plot training history"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    
    ax1.plot(history['train_loss'], label='Train')
    ax1.plot(history['val_loss'], label='Val')
    ax1.set_xlabel('Epoch')
    ax1.set_ylabel('Loss')
    ax1.set_title(f'{name} - Loss')
    ax1.legend()
    
    ax2.plot(history['val_auc'])
    ax2.set_xlabel('Epoch')
    ax2.set_ylabel('AUC')
    ax2.set_title(f'{name} - Validation AUC')
    
    plt.tight_layout()
    plt.savefig(f"{MODELS_DIR}/{name}_history.png")
    print(f"[OK] Saved plot to {MODELS_DIR}/{name}_history.png")

if __name__ == "__main__":
    import sys
    
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║         MOMENTUM ML MODEL TRAINER v0.1.0                      ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    print(f"[*] Device: {DEVICE}")
    print(f"[*] Training data: {TRAINING_DATA_DIR}")
    print(f"[*] Output: {MODELS_DIR}")
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "entry":
            model, scaler, history = train_entry_model()
            plot_history(history, "entry_model")
        elif sys.argv[1] == "exit":
            model, scaler, history = train_exit_model()
            plot_history(history, "exit_model")
        else:
            print(f"Unknown model type: {sys.argv[1]}")
            print("Usage: python train_models.py [entry|exit]")
    else:
        # Train both
        entry_model, entry_scaler, entry_history = train_entry_model()
        plot_history(entry_history, "entry_model")
        
        exit_model, exit_scaler, exit_history = train_exit_model()
        plot_history(exit_history, "exit_model")
        
        print("\n" + "="*60)
        print("TRAINING COMPLETE")
        print("="*60)
        print(f"[OK] Entry model: {MODELS_DIR}/entry_model.txt")
        print(f"[OK] Exit model: {MODELS_DIR}/exit_model.txt")
