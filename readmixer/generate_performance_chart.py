#!/usr/bin/env python3
"""
Generate performance chart for ReadMixer processing on AMD Ryzen 9 9950X system
"""

import matplotlib.pyplot as plt
import numpy as np

# Performance data from actual runs
data = [
    {"reads_millions": 221, "size_gb": 17.3, "time_min": 15.2, "rate_gb_min": 1.14, "label": "Run 1"},
    {"reads_millions": 148, "size_gb": 11.5, "time_min": 9.9, "rate_gb_min": 1.16, "label": "Run 2"}, 
    {"reads_millions": 457, "size_gb": 35.1, "time_min": 30.4, "rate_gb_min": 1.15, "label": "Run 3"}
]

# Extract data for plotting
sizes = [d["size_gb"] for d in data]
times = [d["time_min"] for d in data]
rates = [d["rate_gb_min"] for d in data]
reads = [d["reads_millions"] for d in data]
labels = [d["label"] for d in data]

# Create figure with subplots
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
fig.suptitle('ReadMixer Performance Analysis - AMD Ryzen 9 9950X (32 cores, 96GB RAM)', 
             fontsize=16, fontweight='bold')

# Colors for consistency
colors = ['#1f77b4', '#ff7f0e', '#d62728']  # Blue, Orange, Red
new_run_color = '#d62728'  # Red for the new run

# Plot 1: Processing Time vs Input Size
ax1.scatter(sizes[:-1], times[:-1], c=colors[:-1], s=100, alpha=0.7, label='Previous runs')
ax1.scatter([sizes[-1]], [times[-1]], c=new_run_color, s=150, marker='*', label='Run 3 (35.1GB)', zorder=5)

# Add trend line
z = np.polyfit(sizes, times, 1)
p = np.poly1d(z)
x_trend = np.linspace(min(sizes), max(sizes), 100)
ax1.plot(x_trend, p(x_trend), 'k--', alpha=0.5, label=f'Trend: {z[0]:.2f}x + {z[1]:.1f}')

ax1.set_xlabel('Input Size (GB)')
ax1.set_ylabel('Processing Time (minutes)')
ax1.set_title('Processing Time vs Input Size')
ax1.grid(True, alpha=0.3)
ax1.legend()

# Annotate points
for i, (size, time, label) in enumerate(zip(sizes, times, labels)):
    ax1.annotate(f'{label}\n{size}GB, {time:.1f}min', 
                (size, time), xytext=(5, 5), textcoords='offset points', 
                fontsize=9, alpha=0.8)

# Plot 2: Processing Rate Consistency
ax2.scatter(sizes[:-1], rates[:-1], c=colors[:-1], s=100, alpha=0.7, label='Previous runs')
ax2.scatter([sizes[-1]], [rates[-1]], c=new_run_color, s=150, marker='*', label='Run 3 (35.1GB)', zorder=5)

# Add average line
avg_rate = np.mean(rates)
ax2.axhline(y=avg_rate, color='red', linestyle='--', alpha=0.7, 
           label=f'Average: {avg_rate:.3f} GB/min')

ax2.set_xlabel('Input Size (GB)')
ax2.set_ylabel('Processing Rate (GB/min)')
ax2.set_title('Processing Rate Consistency')
ax2.grid(True, alpha=0.3)
ax2.legend()
ax2.set_ylim(1.10, 1.20)  # Focus on the rate variation

# Annotate points
for i, (size, rate, label) in enumerate(zip(sizes, rates, labels)):
    ax2.annotate(f'{label}\n{rate:.3f} GB/min', 
                (size, rate), xytext=(5, 5), textcoords='offset points', 
                fontsize=9, alpha=0.8)

# Plot 3: Reads vs Processing Time
ax3.scatter(reads[:-1], times[:-1], c=colors[:-1], s=100, alpha=0.7, label='Previous runs')
ax3.scatter([reads[-1]], [times[-1]], c=new_run_color, s=150, marker='*', label='Run 3 (457M reads)', zorder=5)

# Add trend line
z_reads = np.polyfit(reads, times, 1)
p_reads = np.poly1d(z_reads)
x_reads_trend = np.linspace(min(reads), max(reads), 100)
ax3.plot(x_reads_trend, p_reads(x_reads_trend), 'k--', alpha=0.5, 
         label=f'Trend: {z_reads[0]:.3f}x + {z_reads[1]:.1f}')

ax3.set_xlabel('Number of Reads (millions)')
ax3.set_ylabel('Processing Time (minutes)')
ax3.set_title('Reads vs Processing Time')
ax3.grid(True, alpha=0.3)
ax3.legend()

# Annotate points
for i, (read, time, label) in enumerate(zip(reads, times, labels)):
    ax3.annotate(f'{label}\n{read}M reads', 
                (read, time), xytext=(5, 5), textcoords='offset points', 
                fontsize=9, alpha=0.8)

# Plot 4: Performance Summary Table
ax4.axis('tight')
ax4.axis('off')

# Create table data
table_data = []
table_data.append(['Run', 'Reads (M)', 'Size (GB)', 'Time (min)', 'Rate (GB/min)', 'Memory Used'])
for i, d in enumerate(data):
    memory_used = "50.3 GB" if i == 2 else "~50 GB"  # We know exact memory for new run
    table_data.append([
        d['label'], 
        f"{d['reads_millions']}", 
        f"{d['size_gb']}", 
        f"{d['time_min']:.1f}", 
        f"{d['rate_gb_min']:.3f}",
        memory_used
    ])

# Add summary statistics
table_data.append(['', '', '', '', '', ''])
table_data.append(['Average', f"{np.mean(reads):.0f}", f"{np.mean(sizes):.1f}", 
                  f"{np.mean(times):.1f}", f"{np.mean(rates):.3f}", '9 workers'])
table_data.append(['Std Dev', f"{np.std(reads):.0f}", f"{np.std(sizes):.1f}", 
                  f"{np.std(times):.1f}", f"{np.std(rates):.4f}", '5M chunk size'])

table = ax4.table(cellText=table_data, cellLoc='center', loc='center',
                 colWidths=[0.15, 0.15, 0.15, 0.15, 0.18, 0.22])
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 2)

# Style the header row
for i in range(len(table_data[0])):
    table[(0, i)].set_facecolor('#4CAF50')
    table[(0, i)].set_text_props(weight='bold', color='white')

# Style the new run row
for i in range(len(table_data[0])):
    table[(3, i)].set_facecolor('#ffebee')  # Light red background for new run

# Style summary rows
for row in [4, 5, 6]:  # Summary rows
    for i in range(len(table_data[0])):
        table[(row, i)].set_facecolor('#f5f5f5')

ax4.set_title('Performance Summary', fontweight='bold', pad=20)

# Add system info text
system_info = """System Configuration:
• AMD Ryzen 9 9950X @ 4.3GHz (32 cores)
• 96GB RAM (92GB detected, 86GB max for processing)
• 9 workers, 5M reads per chunk
• Consistent ~1.15 GB/min processing rate
• Linear scaling with input size"""

plt.figtext(0.02, 0.02, system_info, fontsize=10, 
           bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.8))

plt.tight_layout()
plt.subplots_adjust(bottom=0.15)  # Make room for system info
plt.savefig('readmixer_performance.png', dpi=300, bbox_inches='tight')
print("Performance chart saved as 'readmixer_performance.png'")
print(f"Average processing rate: {np.mean(rates):.3f} ± {np.std(rates):.4f} GB/min")
print(f"Linear scaling coefficient: {z[0]:.3f} min/GB") 