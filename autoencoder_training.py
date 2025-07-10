import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import pandas as pd
import numpy as np

# Dataset definition
class DemandVectorDataset(Dataset):
    def __init__(self, csv_path):
        df = pd.read_csv(csv_path)
        self.concepts = df['concept'].values  # keep concept labels
        self.data = df.drop(columns=['concept']).values.astype(np.float32)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return torch.tensor(self.data[idx])


# Autoencoder model 
class Autoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim=16, encoded_dim=8):
        super(Autoencoder, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, encoded_dim),
        )
        self.decoder = nn.Sequential(
            nn.Linear(encoded_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, input_dim),
        )

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded


# Main training routine
def train_autoencoder(csv_path, num_epochs=100, batch_size=8, learning_rate=1e-3):
    # Load data
    dataset = DemandVectorDataset(csv_path)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    input_dim = dataset.data.shape[1]

    # Model, loss, optimizer
    model = Autoencoder(input_dim)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    # Training loop
    for epoch in range(num_epochs):
        total_loss = 0.0
        for batch in dataloader:
            optimizer.zero_grad()
            outputs = model(batch)
            loss = criterion(outputs, batch)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        print(f"Epoch [{epoch+1}/{num_epochs}], Loss: {total_loss:.4f}")

    # Save encoder features (compressed vectors)
    all_data = torch.tensor(dataset.data)
    encoded_vectors = model.encoder(all_data).detach().numpy()
    encoded_df = pd.DataFrame(encoded_vectors, columns=[f'feature_{i+1}' for i in range(encoded_vectors.shape[1])])
    encoded_df.insert(0, 'concept', dataset.concepts)
    encoded_df.to_csv('youtube_demand_encoded.csv', index=False)
    print("Encoding completed and saved to 'youtube_demand_encoded.csv'")


# Run
if __name__ == '__main__':
    train_autoencoder('youtube_demand_vector_normalized.csv')
