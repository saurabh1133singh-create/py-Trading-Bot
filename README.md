# UNI-USD Trading Bot

A sophisticated automated trading bot for UNI-USD perpetual futures on Delta Exchange, implementing an EMA (Exponential Moving Average) crossover strategy with advanced risk management features.

## 🚀 Features

### Core Trading Strategy
- **EMA Crossover Strategy**: Uses 50-period EMA for trend identification
- **Dual Price Monitoring**: Candle close prices for signals, ticker prices for execution
- **Pending Order System**: Places orders at EMA ± offset for better entry prices
- **Smart Reversal Logic**: Automatically reverses pending orders on opposite EMA crosses

### Advanced Risk Management
- **Fixed Stop Loss**: 3% fixed stop loss on all positions
- **Multi-Level Trailing Stop Loss**: 
  - 0.47 trail SL activated at 0.5 profit
  - Progressive trailing SLs at 0.2, 0.3, 0.4 profit levels
- **SL Grace Period**: Prevents premature stop loss on entry candle
- **EMA Cross Wait**: Waits for price to cross EMA after exit before new signals

### Technical Features
- **Real-time Price Monitoring**: 2-second ticker updates for fast execution
- **Candle Data Validation**: Ensures data quality and recency
- **Error Handling**: Comprehensive retry logic and fallback mechanisms
- **Position Tracking**: Complete position lifecycle management
- **Logging**: Detailed logging for monitoring and debugging

## 📋 Prerequisites

- Python 3.8+
- Delta Exchange account with API access
- Sufficient balance for trading (minimum 1 lot)

## 🛠️ Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd uni-usd-tbot
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file in the project root:
   ```env
   DELTA_API_KEY=your_api_key_here
   DELTA_API_SECRET=your_api_secret_here
   DELTA_BASE_URL=https://api.india.delta.exchange
   TRADING_SYMBOL=UNIUSD
   TRADING_ASSET_ID=UNI
   TRADING_SIZE=1
   TRADING_LEVERAGE=25
   TRADING_TIMEFRAME=15m
   ```

## 🚀 Usage

### Basic Usage
```bash
python uni-usd-tbot-main-best.py
```

### Backtesting (Optional)
```bash
python uni-usd-tbot-main-best.py --backtest --start 2024-01-01 --end 2024-01-31
```

## ⚙️ Configuration

### Trading Parameters
- **Symbol**: UNIUSD (configurable via environment)
- **Leverage**: 25x (configurable)
- **Position Size**: 1 lot (configurable)
- **Timeframe**: 15 minutes (configurable)
- **EMA Period**: 50 (hardcoded)

### Risk Management Settings
- **Fixed SL**: 3% of entry price
- **EMA Offset**: 0.03 (3% offset for pending orders)
- **Trailing SL Levels**:
  - 0.05 at 0.2 profit
  - 0.10 at 0.3 profit
  - 0.15 at 0.4 profit
  - 0.47 at 0.5 profit

## 📊 Trading Logic

### Entry Strategy
1. **Signal Generation**: Price crosses above/below 50-period EMA
2. **Pending Order**: Places order at EMA ± 0.03 offset
3. **Order Fill**: Executes when ticker price reaches pending price
4. **Reversal**: Reverses pending order if price crosses EMA in opposite direction

### Exit Strategy
1. **Fixed Stop Loss**: 3% loss from entry price
2. **Trailing Stop Loss**: Progressive levels based on profit
3. **Take Profit**: No fixed TP, relies on trailing SL
4. **EMA Cross Wait**: Waits for price to cross EMA after exit

### Position Management
- **Long Positions**: Enter on bullish EMA cross, exit on bearish cross or SL
- **Short Positions**: Enter on bearish EMA cross, exit on bullish cross or SL
- **One Position**: Only one position at a time
- **Grace Period**: SL checks disabled for one candle after entry

## 🔧 Technical Details

### Data Sources
- **Candle Data**: Delta Exchange historical candles API
- **Ticker Data**: Delta Exchange real-time ticker API
- **Price Validation**: Ensures data quality and recency

### Error Handling
- **API Failures**: Automatic retry with exponential backoff
- **Data Validation**: Checks for NaN values and data quality
- **Connection Issues**: Graceful degradation and recovery
- **Order Failures**: Comprehensive error logging and retry logic

### Performance Optimizations
- **Fast Monitoring**: 2-second ticker updates
- **Efficient Calculations**: Optimized EMA calculation
- **Memory Management**: Proper cleanup and resource management
- **Concurrent Processing**: Non-blocking operations

## 📈 Monitoring

### Logging
The bot provides comprehensive logging including:
- Entry/exit signals and reasons
- Position status and P&L
- API calls and responses
- Error messages and warnings
- Performance metrics

### Key Metrics
- **Win Rate**: Percentage of profitable trades
- **Average P&L**: Average profit/loss per trade
- **Max Drawdown**: Maximum loss from peak
- **Sharpe Ratio**: Risk-adjusted returns

## ⚠️ Risk Disclaimer

**This software is for educational and research purposes only. Trading cryptocurrencies and derivatives involves substantial risk of loss and is not suitable for all investors. Past performance does not guarantee future results. Use at your own risk.**

### Important Notes
- Start with small position sizes
- Monitor the bot regularly
- Understand the risks involved
- Test thoroughly before live trading
- Keep API keys secure

## 🐛 Troubleshooting

### Common Issues
1. **API Connection Failed**: Check API keys and network connection
2. **Insufficient Balance**: Ensure adequate account balance
3. **Data Fetch Errors**: Check internet connection and API status
4. **Order Execution Failed**: Verify account permissions and market status

### Debug Mode
Enable debug logging by modifying the logging level:
```python
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For support, please open an issue on GitHub or contact the maintainers.

## 🔄 Version History

- **v1.0.0**: Initial release with basic EMA strategy
- **v1.1.0**: Added trailing stop loss functionality
- **v1.2.0**: Implemented pending order system
- **v1.3.0**: Added EMA cross wait logic
- **v1.4.0**: Enhanced error handling and monitoring

---

**Happy Trading! 🚀📈**
