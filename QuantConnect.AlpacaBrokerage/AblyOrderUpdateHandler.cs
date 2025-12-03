/*
 * VIBE-FI.COM - Ably Order Update Handler
 * 
 * Receives order updates from Ably channels published by the backend.
 * Replaces direct Alpaca WebSocket connection for order status updates.
 */

using System;
using System.Threading.Tasks;
using IO.Ably;
using IO.Ably.Realtime;
using Newtonsoft.Json;
using QuantConnect.Logging;
using QuantConnect.Orders;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Handles order updates received from Ably pub/sub channels.
    /// Subscribes to orders:{userId} channel for real-time order status updates.
    /// </summary>
    public class AblyOrderUpdateHandler : IDisposable
    {
        private AblyRealtime _ablyClient;
        private IRealtimeChannel _orderChannel;
        private readonly string _userId;
        private readonly Action<AblyOrderEvent> _onOrderUpdate;
        private bool _isConnected;

        /// <summary>
        /// Returns whether the handler is connected to Ably
        /// </summary>
        public bool IsConnected => _isConnected;

        /// <summary>
        /// Initializes a new instance of the AblyOrderUpdateHandler
        /// </summary>
        /// <param name="ablyApiKey">Ably API key</param>
        /// <param name="userId">User ID for channel subscription</param>
        /// <param name="projectId">Project ID for client identification</param>
        /// <param name="onOrderUpdate">Callback for order update events</param>
        public AblyOrderUpdateHandler(
            string ablyApiKey,
            string userId,
            string projectId,
            Action<AblyOrderEvent> onOrderUpdate)
        {
            _userId = userId;
            _onOrderUpdate = onOrderUpdate;

            if (string.IsNullOrEmpty(ablyApiKey))
            {
                Log.Error("AblyOrderUpdateHandler: Missing Ably API key");
                return;
            }

            try
            {
                var options = new ClientOptions(ablyApiKey)
                {
                    AutoConnect = true,
                    EchoMessages = false,
                    ClientId = $"lean-orders-{projectId}"
                };

                _ablyClient = new AblyRealtime(options);

                _ablyClient.Connection.On(ConnectionEvent.Connected, args =>
                {
                    Log.Trace("AblyOrderUpdateHandler: Connected to Ably");
                    _isConnected = true;
                    SubscribeToOrderChannel();
                });

                _ablyClient.Connection.On(ConnectionEvent.Disconnected, args =>
                {
                    Log.Trace("AblyOrderUpdateHandler: Disconnected from Ably");
                    _isConnected = false;
                });

                _ablyClient.Connection.On(ConnectionEvent.Failed, args =>
                {
                    Log.Error($"AblyOrderUpdateHandler: Connection failed - {args.Reason?.Message}");
                    _isConnected = false;
                });

                Log.Trace($"AblyOrderUpdateHandler: Initialized for user {userId}");
            }
            catch (Exception ex)
            {
                Log.Error($"AblyOrderUpdateHandler: Failed to initialize - {ex.Message}");
            }
        }

        private void SubscribeToOrderChannel()
        {
            var channelName = $"orders:{_userId}";
            _orderChannel = _ablyClient.Channels.Get(channelName);

            _orderChannel.Subscribe("update", message =>
            {
                try
                {
                    ProcessOrderMessage(message);
                }
                catch (Exception ex)
                {
                    Log.Error($"AblyOrderUpdateHandler: Error processing order update - {ex.Message}");
                }
            });

            Log.Trace($"AblyOrderUpdateHandler: Subscribed to {channelName}");
        }

        private void ProcessOrderMessage(Message message)
        {
            var orderEvent = JsonConvert.DeserializeObject<AblyOrderEvent>(message.Data.ToString());
            if (orderEvent == null)
            {
                return;
            }

            Log.Trace($"AblyOrderUpdateHandler: Received order update - {orderEvent.Event} for {orderEvent.Symbol}");
            _onOrderUpdate?.Invoke(orderEvent);
        }

        /// <summary>
        /// Disposes of the Ably connection
        /// </summary>
        public void Dispose()
        {
            _orderChannel?.Unsubscribe();
            _ablyClient?.Close();
            Log.Trace("AblyOrderUpdateHandler: Disposed");
        }
    }

    /// <summary>
    /// Order event data structure from Ably
    /// </summary>
    public class AblyOrderEvent
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("userId")]
        public string UserId { get; set; }

        [JsonProperty("botId")]
        public string BotId { get; set; }

        [JsonProperty("event")]
        public string Event { get; set; }

        [JsonProperty("orderId")]
        public string OrderId { get; set; }

        [JsonProperty("symbol")]
        public string Symbol { get; set; }

        [JsonProperty("side")]
        public string Side { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("quantity")]
        public decimal Quantity { get; set; }

        [JsonProperty("filledQuantity")]
        public decimal FilledQuantity { get; set; }

        [JsonProperty("filledPrice")]
        public decimal? FilledPrice { get; set; }

        [JsonProperty("timestamp")]
        public string Timestamp { get; set; }

        /// <summary>
        /// Converts the Ably event status to LEAN OrderStatus
        /// </summary>
        public Orders.OrderStatus ToLeanOrderStatus()
        {
            return Status?.ToLowerInvariant() switch
            {
                "new" => Orders.OrderStatus.New,
                "submitted" => Orders.OrderStatus.Submitted,
                "partially_filled" => Orders.OrderStatus.PartiallyFilled,
                "filled" => Orders.OrderStatus.Filled,
                "canceled" => Orders.OrderStatus.Canceled,
                "rejected" => Orders.OrderStatus.Invalid,
                "pending_cancel" => Orders.OrderStatus.CancelPending,
                "pending_new" => Orders.OrderStatus.New,
                _ => Orders.OrderStatus.None
            };
        }
    }
}

