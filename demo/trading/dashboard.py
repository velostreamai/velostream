#!/usr/bin/env python3
"""
Velostream Trading Dashboard
Real-time visualization of financial trading data
"""

import json
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import argparse

try:
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    from matplotlib.dates import DateFormatter
    import pandas as pd
    from kafka import KafkaConsumer
    import numpy as np
    HAS_DEPS = True
except ImportError as e:
    HAS_DEPS = False
    MISSING_DEP = str(e)

@dataclass
class MarketData:
    symbol: str
    exchange: str
    price: float
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    volume: int
    timestamp: int
    
    @classmethod
    def from_json(cls, data: dict):
        return cls(**data)

@dataclass
class Alert:
    type: str
    symbol: str
    message: str
    timestamp: int
    data: dict

class TradingDashboard:
    def __init__(self, kafka_brokers: str = "localhost:9092"):
        if not HAS_DEPS:
            raise ImportError(f"Missing dependencies: {MISSING_DEP}")
            
        self.kafka_brokers = kafka_brokers
        self.market_data = defaultdict(lambda: deque(maxlen=100))
        self.alerts = deque(maxlen=50)
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.volume_history = defaultdict(lambda: deque(maxlen=100))
        self.running = True
        
        # Setup matplotlib
        plt.style.use('dark_background')
        self.fig, self.axes = plt.subplots(2, 2, figsize=(15, 10))
        self.fig.suptitle('Velostream Trading Dashboard', fontsize=16, color='white')
        
        # Price chart (top-left)
        self.price_ax = self.axes[0, 0]
        self.price_ax.set_title('Real-time Stock Prices', color='white')
        self.price_ax.set_xlabel('Time', color='white')
        self.price_ax.set_ylabel('Price ($)', color='white')
        
        # Volume chart (top-right)
        self.volume_ax = self.axes[0, 1]
        self.volume_ax.set_title('Trading Volume', color='white')
        self.volume_ax.set_xlabel('Time', color='white')
        self.volume_ax.set_ylabel('Volume', color='white')
        
        # Alerts panel (bottom-left)
        self.alerts_ax = self.axes[1, 0]
        self.alerts_ax.set_title('Live Alerts', color='white')
        self.alerts_ax.axis('off')
        
        # Statistics panel (bottom-right)
        self.stats_ax = self.axes[1, 1]
        self.stats_ax.set_title('Market Statistics', color='white')
        self.stats_ax.axis('off')
        
        plt.tight_layout()
        
    def setup_consumers(self):
        """Setup Kafka consumers for different data streams"""
        self.consumers = {}
        
        # Market data consumer
        self.consumers['market_data'] = KafkaConsumer(
            'market_data',
            bootstrap_servers=self.kafka_brokers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        
        # Alert consumers
        alert_topics = [
            'price_alerts',
            'volume_spikes', 
            'risk_alerts',
            'order_imbalance_alerts',
            'arbitrage_opportunities'
        ]
        
        for topic in alert_topics:
            self.consumers[topic] = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_brokers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
    
    def consume_market_data(self):
        """Consume and process market data"""
        try:
            # Use poll with timeout to avoid blocking
            messages = self.consumers['market_data'].poll(timeout_ms=100)
            for topic_partition, records in messages.items():
                for message in records:
                    if not self.running:
                        return
                        
                    data = MarketData.from_json(message.value)
                    self.market_data[data.symbol].append(data)
                    
                    # Update price history
                    timestamp = datetime.fromtimestamp(data.timestamp / 1000)
                    self.price_history[data.symbol].append((timestamp, data.price))
                    self.volume_history[data.symbol].append((timestamp, data.volume))
                
        except Exception as e:
            pass  # Ignore consumer errors during polling
    
    def consume_alerts(self):
        """Consume and process alerts from various topics"""
        alert_topics = ['price_alerts', 'volume_spikes', 'risk_alerts', 
                       'order_imbalance_alerts', 'arbitrage_opportunities']
        
        for topic in alert_topics:
            try:
                if topic in self.consumers:
                    # Use poll with timeout to avoid blocking
                    messages = self.consumers[topic].poll(timeout_ms=100)
                    for topic_partition, records in messages.items():
                        for message in records:
                            if not self.running:
                                return
                                
                            alert = Alert(
                                type=topic,
                                symbol=message.value.get('symbol', 'N/A'),
                                message=self.format_alert_message(topic, message.value),
                                timestamp=message.value.get('detection_time', 
                                                           message.value.get('spike_time',
                                                           message.value.get('analysis_time',
                                                           message.value.get('opportunity_time', 0)))),
                                data=message.value
                            )
                            self.alerts.append(alert)
                    
            except Exception as e:
                pass  # Ignore consumer errors during polling
    
    def format_alert_message(self, alert_type: str, data: dict) -> str:
        """Format alert messages for display"""
        symbol = data.get('symbol', 'N/A')
        
        if alert_type == 'price_alerts':
            change = data.get('price_change_pct', 0)
            return f"{symbol}: Price moved {change:.2f}%"
            
        elif alert_type == 'volume_spikes':
            ratio = data.get('volume_ratio', 0)
            return f"{symbol}: Volume spike {ratio:.1f}x normal"
            
        elif alert_type == 'risk_alerts':
            status = data.get('risk_status', 'UNKNOWN')
            trader = data.get('trader_id', 'N/A')
            return f"{trader}: {status} for {symbol}"
            
        elif alert_type == 'order_imbalance_alerts':
            buy_ratio = data.get('buy_ratio', 0)
            return f"{symbol}: Order imbalance {buy_ratio:.1%} buy"
            
        elif alert_type == 'arbitrage_opportunities':
            spread_bps = data.get('spread_bps', 0)
            return f"{symbol}: Arbitrage {spread_bps:.1f} bps"
            
        return f"{symbol}: {alert_type}"
    
    def update_plots(self, frame):
        """Update dashboard plots"""
        if not self.running:
            return
        
        # Consume new data
        self.consume_market_data()
        self.consume_alerts()
            
        # Clear all axes
        for ax in self.axes.flat:
            ax.clear()
        
        # Update price chart
        self.price_ax.set_title('Real-time Stock Prices', color='white')
        self.price_ax.set_xlabel('Time', color='white')
        self.price_ax.set_ylabel('Price ($)', color='white')
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(self.price_history)))
        for i, (symbol, history) in enumerate(self.price_history.items()):
            if history:
                times, prices = zip(*history)
                self.price_ax.plot(times, prices, label=symbol, color=colors[i], linewidth=2)
        
        if self.price_history:
            self.price_ax.legend(loc='upper left', fancybox=True, framealpha=0.9)
            self.price_ax.grid(True, alpha=0.3)
        
        # Update volume chart
        self.volume_ax.set_title('Trading Volume', color='white')
        self.volume_ax.set_xlabel('Time', color='white')
        self.volume_ax.set_ylabel('Volume', color='white')
        
        for i, (symbol, history) in enumerate(self.volume_history.items()):
            if history:
                times, volumes = zip(*history)
                self.volume_ax.plot(times, volumes, label=symbol, color=colors[i], alpha=0.7)
        
        if self.volume_history:
            self.volume_ax.legend(loc='upper left', fancybox=True, framealpha=0.9)
            self.volume_ax.grid(True, alpha=0.3)
        
        # Update alerts panel
        self.alerts_ax.set_title('Live Alerts', color='white')
        self.alerts_ax.axis('off')
        
        if self.alerts:
            alert_text = []
            for i, alert in enumerate(list(self.alerts)[-10:]):  # Show last 10 alerts
                timestamp = datetime.fromtimestamp(alert.timestamp / 1000) if alert.timestamp else datetime.now()
                time_str = timestamp.strftime('%H:%M:%S')
                alert_text.append(f"{time_str} | {alert.message}")
            
            self.alerts_ax.text(0.05, 0.95, '\n'.join(alert_text), 
                               transform=self.alerts_ax.transAxes,
                               fontsize=8, color='white', 
                               verticalalignment='top',
                               fontfamily='monospace')
        
        # Update statistics panel
        self.stats_ax.set_title('Market Statistics', color='white')
        self.stats_ax.axis('off')
        
        if self.market_data:
            stats_text = []
            stats_text.append("LIVE MARKET DATA")
            stats_text.append("-" * 20)
            
            for symbol, data_points in self.market_data.items():
                if data_points:
                    latest = data_points[-1]
                    stats_text.append(f"{symbol:>6}: ${latest.price:>8.2f}")
                    stats_text.append(f"       Bid: ${latest.bid_price:>8.2f}")
                    stats_text.append(f"       Ask: ${latest.ask_price:>8.2f}")
                    stats_text.append("")
            
            stats_text.append(f"Active Symbols: {len(self.market_data)}")
            stats_text.append(f"Total Alerts: {len(self.alerts)}")
            
            self.stats_ax.text(0.05, 0.95, '\n'.join(stats_text), 
                              transform=self.stats_ax.transAxes,
                              fontsize=9, color='white',
                              verticalalignment='top',
                              fontfamily='monospace')
        
        plt.tight_layout()
    
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signal"""
        print("\nShutting down dashboard...")
        self.running = False
        plt.close('all')
    
    def run(self):
        """Run the dashboard"""
        if not HAS_DEPS:
            print(f"Missing dependencies: {MISSING_DEP}")
            print("Install with: pip install matplotlib pandas kafka-python numpy")
            return
        
        print("🏦 Starting Velostream Trading Dashboard...")
        print(f"📡 Connecting to Kafka: {self.kafka_brokers}")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        try:
            # Setup Kafka consumers
            self.setup_consumers()
            print("✅ Kafka consumers ready")
            
            # Setup matplotlib animation
            ani = animation.FuncAnimation(
                self.fig, self.update_plots, interval=2000, cache_frame_data=False
            )
            
            print("📊 Dashboard starting...")
            print("Press Ctrl+C to stop")
            
            # Show the dashboard
            plt.show()
            
        except KeyboardInterrupt:
            print("\n⏹️  Dashboard stopped by user")
        except Exception as e:
            print(f"❌ Dashboard error: {e}")
        finally:
            self.running = False
            for consumer in self.consumers.values():
                consumer.close()

def main():
    parser = argparse.ArgumentParser(description='Velostream Trading Dashboard')
    parser.add_argument('--brokers', default='localhost:9092',
                       help='Kafka brokers (default: localhost:9092)')
    
    args = parser.parse_args()
    
    dashboard = TradingDashboard(args.brokers)
    dashboard.run()

if __name__ == '__main__':
    main()