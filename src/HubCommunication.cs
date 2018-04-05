
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Shared;
    using Newtonsoft.Json;
    using Opc.Ua;
    using System;
    using System.IO;
    using static OpcPublisher.OpcMonitoredItem;
    using static OpcPublisher.PublisherTelemetryConfiguration;
    using static Program;

    /// <summary>
    /// Class to handle all IoTHub/EdgeHub communication.
    /// </summary>
    public class HubCommunication
    {
        public class IotCentralMessage
        {
            public string Key;
            public string Value;

            public IotCentralMessage()
            {
                Key = null;
                Value = null;
            }
        }

        public static long MonitoredItemsQueueCount => _monitoredItemsDataQueue.Count;

        public static long DequeueCount => _dequeueCount;

        public static long MissedSendIntervalCount => _missedSendIntervalCount;

        public static long TooLargeCount => _tooLargeCount;

        public static long SentBytes => _sentBytes;

        public static long SentMessages => _sentMessages;

        public static DateTime SentLastTime => _sentLastTime;

        public static long FailedMessages => _failedMessages;

        public const uint HubMessageSizeMax = (256 * 1024);

        public static uint HubMessageSize
        {
            get => _hubMessageSize;
            set => _hubMessageSize = value;
        }

        public static Microsoft.Azure.Devices.Client.TransportType HubProtocol
        {
            get => _hubProtocol;
            set => _hubProtocol = value;
        }

        public static int DefaultSendIntervalSeconds
        {
            get => _defaultSendIntervalSeconds;
            set => _defaultSendIntervalSeconds = value;
        }


        public static int MonitoredItemsQueueCapacity
        {
            get => _monitoredItemsQueueCapacity;
            set => _monitoredItemsQueueCapacity = value;
        }

        public static long EnqueueCount
        {
            get => _enqueueCount;
        }

        public static long EnqueueFailureCount
        {
            get => _enqueueFailureCount;
        }

        public static bool IsHttp1Transport() => (_transportType == Microsoft.Azure.Devices.Client.TransportType.Http1);

        public static bool IotCentralMode
        {
            get => _iotCentralMode;
            set => _iotCentralMode = value;
        }


        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubCommunication(CancellationToken ct)
        {
            _shutdownToken = ct;
        }

        /// <summary>
        /// Initializes message broker communication.
        /// </summary>
        public async Task<bool> InitHubCommunicationAsync(DeviceClient hubClient, Microsoft.Azure.Devices.Client.TransportType transportType)
        {
            try
            {
                // set hub communication parameters
                _hubClient = hubClient;
                _transportType = transportType;
                _hubClient.ProductInfo = "OpcPublisher";
                ExponentialBackoff exponentialRetryPolicy = new ExponentialBackoff(int.MaxValue, TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(1024), TimeSpan.FromMilliseconds(3));
                _hubClient.SetRetryPolicy(exponentialRetryPolicy);

                // register connection status change handler
                _hubClient.SetConnectionStatusChangesHandler(ConnectionStatusChange);

                // show IoTCentral mode
                Logger.Information($"IoTCentral mode: {_iotCentralMode}");

                // open connection
                Logger.Debug($"Open hub communication");
                await _hubClient.OpenAsync();

                // twin properties and methods are not supported for HTTP
                if (!IsHttp1Transport())
                {
                    Logger.Debug($"Register desired properties and method callbacks");

                    // register property update handler
                    await _hubClient.SetDesiredPropertyUpdateCallbackAsync(DesiredPropertiesUpdate, null);

                    // register method handlers
                    await _hubClient.SetMethodHandlerAsync("PublishNode", HandlePublishNodeMethodAsync, hubClient);
                    await _hubClient.SetMethodHandlerAsync("UnpublishNode", HandleUnpublishNodeMethodAsync, hubClient);
                    await _hubClient.SetMethodHandlerAsync("GetListOfPublishedNodes", HandleGetListOfPublishedNodesMethodAsync, hubClient);
                    // todo
                    //await _hubClient.SetMethodHandlerAsync("ResetPublishedNodes", HandleResetPublishedNodesMethodAsync, hubClient);
                }

                // C2D messages are supported for all protocols, HTTP requires polling
                Logger.Debug($"Init C2D messaging");
                InitC2dMessaging();

                Logger.Debug($"Init D2C message processing");
                return await InitMessageProcessingAsync();
            }
            catch (Exception e)
            {
                Logger.Error(e, "Failure initializing hub communication processing.");
                return false;
            }
        }

        private async Task C2dReceiveMessageHandlerAsync()
        {
            while (true)
            {
                if (_shutdownToken.IsCancellationRequested)
                {
                    break;
                }
                Microsoft.Azure.Devices.Client.Message message = await _hubClient.ReceiveAsync(TimeSpan.FromSeconds(60));
                if (message != null)
                {
                    Logger.Debug($"Got message from IoTHub: {message.ToString()}");
                    // todo - process message
                    await _hubClient.CompleteAsync(message);
                }
            }
        }

        // todo remove
        //private async Task C2dAnnounceAsync()
        //{
        //    DateTime startTime = DateTime.UtcNow;
        //    DateTime lastAnnounce = new DateTime(0);
        //    bool announce = true;
        //    TimeSpan shortAnnouncementPeriod = new TimeSpan(0, 0, 30);
        //    TimeSpan regularAnnouncementPeriod = new TimeSpan(1, 0, 0);
        //    TimeSpan startAnnouncementPhase = new TimeSpan(1, 0, 0);

        //    // in the first hour we announce each 5 minutes, later each hour
        //    while (true)
        //    {
        //        if (_shutdownToken.IsCancellationRequested)
        //        {
        //            break;
        //        }
        //        if (announce)
        //        {
        //            var announceMessage = new Microsoft.Azure.Devices.Client.Message(Encoding.ASCII.GetBytes($"'ApplicationUri': '{ApplicationUriNameDefault}'"));
        //            await _hubClient.SendEventAsync(announceMessage);
        //            if (GetType().IsInstanceOfType(typeof(IotEdgeHubCommunication)))
        //            {
        //                await _hubClient.SendEventAsync("OpcAnnouncements", announceMessage);
        //            }
        //            lastAnnounce = DateTime.UtcNow;
        //        }
        //        await Task.Delay(shortAnnouncementPeriod);
        //        announce = false;
        //        if (DateTime.UtcNow - startTime < startAnnouncementPhase)
        //        {
        //            if (DateTime.UtcNow - lastAnnounce > shortAnnouncementPeriod)
        //            {
        //                announce = true;
        //            }
        //        }
        //        else
        //        {
        //            if (DateTime.UtcNow - lastAnnounce > regularAnnouncementPeriod)
        //            {
        //                announce = true;
        //            }
        //        }
        //    }
        //}

        private void InitC2dMessaging()
        {
            // create a task to poll for messages
            Task.Run(async () => await C2dReceiveMessageHandlerAsync());

            // todo remove
            // announce ourselve
            //Task.Run(async () => await C2dAnnounceAsync());
        }

        private Task DesiredPropertiesUpdate(TwinCollection desiredProperties, object userContext)
        {
            try
            {
                Logger.Debug("Desired property update:");
                Logger.Debug(JsonConvert.SerializeObject(desiredProperties));

                // todo - add handling
            }
            catch (AggregateException e)
            {
                foreach (Exception ex in e.InnerExceptions)
                {
                    Logger.Error(ex, "Error in desired property update.");
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Error in desired property update.");
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Handle connection status change notifications.
        /// </summary>
        static void ConnectionStatusChange(ConnectionStatus status, ConnectionStatusChangeReason reason)
        {
                Logger.Information($"Connection status changed to '{status}', reason '{reason}'");
        }

        /// <summary>
        /// Handle publish node method calls.
        /// </summary>
        static async Task<MethodResponse> HandlePublishNodeMethodAsync(MethodRequest methodRequest, object userContext)
        {
            MethodResponse response = new MethodResponse(0);
            try
            {
                Logger.Debug("PublishNode method called.");

                // todo - process publish request

                // Indicate that the message treatment is completed
                return response;
            }
            catch (AggregateException e)
            {
                foreach (Exception ex in e.InnerExceptions)
                {
                    Logger.Error(ex, "Error in PublishNode method handler.");
                }
                // Indicate that the message treatment is not completed
                DeviceClient hubClient = (DeviceClient)userContext;
                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e, "Error in PublishNode method handler.");
                // Indicate that the message treatment is not completed
                DeviceClient hubClient = (DeviceClient)userContext;
                return response;
            }
        }


        /// <summary>
        /// Handle unpublish node method calls.
        /// </summary>
        static async Task<MethodResponse> HandleUnpublishNodeMethodAsync(MethodRequest methodRequest, object userContext)
        {
            MethodResponse response = new MethodResponse(0);
            try
            {
                Logger.Debug("UnpublishNode method called.");

                // todo - process unpublish request

                // Indicate that the message treatment is completed
                return response;
            }
            catch (AggregateException e)
            {
                foreach (Exception ex in e.InnerExceptions)
                {
                    Logger.Error(ex, "Error in UnpublishNode method handler.");
                }
                // Indicate that the message treatment is not completed
                DeviceClient hubClient = (DeviceClient)userContext;
                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e, "Error in UnpublishNode method handler.");
                // Indicate that the message treatment is not completed
                DeviceClient hubClient = (DeviceClient)userContext;
                return response;
            }
        }


        /// <summary>
        /// Handle get list of published nodes method calls.
        /// </summary>
        static async Task<MethodResponse> HandleGetListOfPublishedNodesMethodAsync(MethodRequest methodRequest, object userContext)
        {
            MethodResponse response = new MethodResponse(0);
            try
            {
                Logger.Debug("GetListOfPublishedNodes method called.");

                // todo - process request

                // Indicate that the message treatment is completed
                return response;
            }
            catch (AggregateException e)
            {
                foreach (Exception ex in e.InnerExceptions)
                {
                    Logger.Error(ex, "Error in GetListOfPublishedNodes method handler.");
                }
                // Indicate that the message treatment is not completed
                DeviceClient hubClient = (DeviceClient)userContext;
                return response;
            }
            catch (Exception e)
            {
                Logger.Error(e, "Error in GetListOfPublishedNodes method handler.");
                // Indicate that the message treatment is not completed
                DeviceClient hubClient = (DeviceClient)userContext;
                return response;
            }
        }

        /// <summary>
        /// Initializes internal message processing.
        /// </summary>
        public async Task<bool> InitMessageProcessingAsync()
        {
            try
            {
                // show config
                Logger.Information($"Message processing and hub communication configured with a send interval of {_defaultSendIntervalSeconds} sec and a message buffer size of {_hubMessageSize} bytes.");

                // create the queue for monitored items
                _monitoredItemsDataQueue = new BlockingCollection<MessageData>(_monitoredItemsQueueCapacity);

                // start up task to send telemetry to IoTHub
                _monitoredItemsProcessorTask = null;

                Logger.Information("Creating task process and batch monitored item data updates...");
                _monitoredItemsProcessorTask = Task.Run(async () => await MonitoredItemsProcessorAsync(_shutdownToken), _shutdownToken);
                return true;
            }
            catch (Exception e)
            {
                Logger.Error(e, "Failure initializing message processing.");
                return false;
            }
        }

        /// <summary>
        /// Shuts down the IoTHub communication.
        /// </summary>
        public async Task ShutdownAsync()
        {
            // send cancellation token and wait for last IoT Hub message to be sent.
            try
            {
                await _monitoredItemsProcessorTask;

                if (_hubClient != null)
                {
                    await _hubClient.CloseAsync();
                }

                _monitoredItemsDataQueue = null;
                _monitoredItemsProcessorTask = null;
                _hubClient = null;
            }
            catch (Exception e)
            {
                Logger.Error(e, "Failure while shutting down hub messaging.");
            }
        }

        /// <summary>
        /// Enqueue a message for sending to IoTHub.
        /// </summary>
        public static void Enqueue(MessageData json)
        {
            // Try to add the message.
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredItemsDataQueue.TryAdd(json) == false)
            {
                Interlocked.Increment(ref _enqueueFailureCount);
                if (_enqueueFailureCount % 10000 == 0)
                {
                    Logger.Information($"The internal monitored item message queue is above its capacity of {_monitoredItemsDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
                }
            }
        }

        /// <summary>
        /// Creates a JSON message to be sent to IoTHub, based on the telemetry configuration for the endpoint.
        /// </summary>
        private async Task<string> CreateJsonMessageAsync(MessageData messageData)
        {
            try
            {
                // get telemetry configration
                EndpointTelemetryConfiguration telemetryConfiguration = GetEndpointTelemetryConfiguration(messageData.EndpointUrl);

                // currently the pattern processing is done in MonitoredItem_Notification of OpcSession.cs. in case of perf issues
                // it can be also done here, the risk is then to lose messages in the communication queue. if you enable it here, disable it in OpcSession.cs
                // messageData.ApplyPatterns(telemetryConfiguration);

                // build the JSON message
                StringBuilder _jsonStringBuilder = new StringBuilder();
                StringWriter _jsonStringWriter = new StringWriter(_jsonStringBuilder);
                using (JsonWriter _jsonWriter = new JsonTextWriter(_jsonStringWriter))
                {
                    await _jsonWriter.WriteStartObjectAsync();
                    string telemetryValue = string.Empty;

                    // process EndpointUrl
                    if ((bool)telemetryConfiguration.EndpointUrl.Publish)
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.EndpointUrl.Name);
                        await _jsonWriter.WriteValueAsync(messageData.EndpointUrl);
                    }

                    // process NodeId
                    if (!string.IsNullOrEmpty(messageData.NodeId))
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.NodeId.Name);
                        await _jsonWriter.WriteValueAsync(messageData.NodeId);
                    }

                    // process MonitoredItem object properties
                    if (!string.IsNullOrEmpty(messageData.ApplicationUri) || !string.IsNullOrEmpty(messageData.DisplayName))
                    {
                        if (!(bool)telemetryConfiguration.MonitoredItem.Flat)
                        {
                            await _jsonWriter.WritePropertyNameAsync("MonitoredItem");
                            await _jsonWriter.WriteStartObjectAsync();
                        }

                        // process ApplicationUri
                        if (!string.IsNullOrEmpty(messageData.ApplicationUri))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.ApplicationUri.Name);
                            await _jsonWriter.WriteValueAsync(messageData.ApplicationUri);
                        }

                        // process DisplayName
                        if (!string.IsNullOrEmpty(messageData.DisplayName))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.DisplayName.Name);
                            await _jsonWriter.WriteValueAsync(messageData.DisplayName);
                        }

                        if (!(bool)telemetryConfiguration.MonitoredItem.Flat)
                        {
                            await _jsonWriter.WriteEndObjectAsync();
                        }
                    }

                    // process Value object properties
                    if (!string.IsNullOrEmpty(messageData.Value) || !string.IsNullOrEmpty(messageData.SourceTimestamp) ||
                       messageData.StatusCode != null || !string.IsNullOrEmpty(messageData.Status))
                    {
                        if (!(bool)telemetryConfiguration.Value.Flat)
                        {
                            await _jsonWriter.WritePropertyNameAsync("Value");
                            await _jsonWriter.WriteStartObjectAsync();
                        }

                        // process Value
                        if (!string.IsNullOrEmpty(messageData.Value))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.Value.Name);
                            if (messageData.PreserveValueQuotes)
                            {
                                await _jsonWriter.WriteValueAsync(messageData.Value);
                            }
                            else
                            {
                                await _jsonWriter.WriteRawValueAsync(messageData.Value);
                            }
                        }

                        // process SourceTimestamp
                        if (!string.IsNullOrEmpty(messageData.SourceTimestamp))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.SourceTimestamp.Name);
                            await _jsonWriter.WriteValueAsync(messageData.SourceTimestamp);
                        }

                        // process StatusCode
                        if (messageData.StatusCode != null)
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.StatusCode.Name);
                            await _jsonWriter.WriteValueAsync(messageData.StatusCode);
                        }

                        // process Status
                        if (!string.IsNullOrEmpty(messageData.Status))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.Status.Name);
                            await _jsonWriter.WriteValueAsync(messageData.Status);
                        }

                        if (!(bool)telemetryConfiguration.Value.Flat)
                        {
                            await _jsonWriter.WriteEndObjectAsync();
                        }
                    }
                    await _jsonWriter.WriteEndObjectAsync();
                    await _jsonWriter.FlushAsync();
                }
                return _jsonStringBuilder.ToString();
            }
            catch (Exception e)
            {
                Logger.Error(e, "Generation of JSON message failed.");
            }
            return string.Empty;
        }

        /// <summary>
        /// Creates a JSON message to be sent to IoTCentral.
        /// </summary>
        private async Task<string> CreateIotCentralJsonMessageAsync(MessageData messageData)
        {
            try
            {
                // build the JSON message for IoTCentral
                StringBuilder _jsonStringBuilder = new StringBuilder();
                StringWriter _jsonStringWriter = new StringWriter(_jsonStringBuilder);
                using (JsonWriter _jsonWriter = new JsonTextWriter(_jsonStringWriter))
                {
                    await _jsonWriter.WriteStartObjectAsync();
                    await _jsonWriter.WritePropertyNameAsync(messageData.DisplayName);
                    await _jsonWriter.WriteValueAsync(messageData.Value);
                    await _jsonWriter.WriteEndObjectAsync();
                    await _jsonWriter.FlushAsync();
                }
                return _jsonStringBuilder.ToString();
            }
            catch (Exception e)
            {
                Logger.Error(e, "Generation of IoTCentral JSON message failed.");
            }
            return string.Empty;
        }

        /// <summary>
        /// Dequeue monitored item notification messages, batch them for send (if needed) and send them to IoTHub.
        /// </summary>
        protected async Task MonitoredItemsProcessorAsync(CancellationToken ct)
        {
            uint jsonSquareBracketLength = 2;
            Microsoft.Azure.Devices.Client.Message tempMsg = new Microsoft.Azure.Devices.Client.Message();
            // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
            int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString().Length;
            // if batching is requested the buffer will have the requested size, otherwise we reserve the max size
            uint hubMessageBufferSize = (_hubMessageSize > 0 ? _hubMessageSize : HubMessageSizeMax) - (uint)systemPropertyLength - (uint)jsonSquareBracketLength;
            byte[] hubMessageBuffer = new byte[hubMessageBufferSize];
            MemoryStream hubMessage = new MemoryStream(hubMessageBuffer);
            DateTime nextSendTime = DateTime.UtcNow + TimeSpan.FromSeconds(_defaultSendIntervalSeconds);
            double millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;

            using (hubMessage)
            {
                try
                {
                    string jsonMessage = string.Empty;
                    MessageData messageData = new MessageData();
                    bool needToBufferMessage = false;
                    int jsonMessageSize = 0;

                    hubMessage.Position = 0;
                    hubMessage.SetLength(0);
                    hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                    while (true)
                    {
                        // sanity check the send interval, compute the timeout and get the next monitored item message
                        if (_defaultSendIntervalSeconds > 0)
                        {
                            millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                            if (millisecondsTillNextSend < 0)
                            {
                                _missedSendIntervalCount++;
                                // do not wait if we missed the send interval
                                millisecondsTillNextSend = 0;
                            }
                        }
                        else
                        {
                            // if we are in shutdown do not wait, else wait infinite if send interval is not set
                            millisecondsTillNextSend = ct.IsCancellationRequested ? 0 : Timeout.Infinite;
                        }
                        bool gotItem = _monitoredItemsDataQueue.TryTake(out messageData, (int)millisecondsTillNextSend, ct);

                        // the two commandline parameter --ms (message size) and --si (send interval) control when data is sent to IoTHub/EdgeHub
                        // pls see detailed comments on performance and memory consumption at https://github.com/Azure/iot-edge-opc-publisher

                        // check if we got an item or if we hit the timeout or got canceled
                        if (gotItem)
                        {
                            if (IotCentralMode)
                            {
                                // for IoTCentral we send simple key/value pairs. key is the DisplayName, value the value.
                                jsonMessage = await CreateIotCentralJsonMessageAsync(messageData);
                            }
                            else
                            {
                                // create a JSON message from the messageData object
                                jsonMessage = await CreateJsonMessageAsync(messageData);
                            }

                            // todo - send the message to the output route in case we are on edge

                            _dequeueCount++;
                            jsonMessageSize = Encoding.UTF8.GetByteCount(jsonMessage.ToString());

                            // sanity check that the user has set a large enough messages size
                            if ((_hubMessageSize > 0 && jsonMessageSize > _hubMessageSize ) || (_hubMessageSize == 0 && jsonMessageSize > hubMessageBufferSize))
                            {
                                Logger.Error($"There is a telemetry message (size: {jsonMessageSize}), which will not fit into an hub message (max size: {hubMessageBufferSize}].");
                                Logger.Error($"Please check your hub message size settings. The telemetry message will be discarded silently. Sorry:(");
                                _tooLargeCount++;
                                continue;
                            }

                            // if batching is requested or we need to send at intervals, batch it otherwise send it right away
                            needToBufferMessage = false;
                            if (_hubMessageSize > 0 || (_hubMessageSize == 0 && _defaultSendIntervalSeconds > 0))
                            {
                                // if there is still space to batch, do it. otherwise send the buffer and flag the message for later buffering
                                if (hubMessage.Position + jsonMessageSize + 1 <= hubMessage.Capacity)
                                {
                                    // add the message and a comma to the buffer
                                    hubMessage.Write(Encoding.UTF8.GetBytes(jsonMessage.ToString()), 0, jsonMessageSize);
                                    hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                    Logger.Debug($"Added new message with size {jsonMessageSize} to hub message (size is now {(hubMessage.Position - 1)}).");
                                    continue;
                                }
                                else
                                {
                                    needToBufferMessage = true;
                                }
                            }
                        }
                        else
                        {
                            // if we got no message, we either reached the interval or we are in shutdown and have processed all messages
                            if (ct.IsCancellationRequested)
                            {
                                Logger.Information($"Cancellation requested.");
                                _monitoredItemsDataQueue.CompleteAdding();
                                _monitoredItemsDataQueue.Dispose();
                                break;
                            }
                        }

                        // the batching is completed or we reached the send interval or got a cancelation request
                        try
                        {
                            Microsoft.Azure.Devices.Client.Message encodedhubMessage = null;

                            // if we reached the send interval, but have nothing to send (only the opening square bracket is there), we continue
                            if (!gotItem && hubMessage.Position == 1)
                            {
                                nextSendTime += TimeSpan.FromSeconds(_defaultSendIntervalSeconds);
                                hubMessage.Position = 0;
                                hubMessage.SetLength(0);
                                hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                                continue;
                            }

                            // if there is no batching and not interval configured, we send the JSON message we just got, otherwise we send the buffer
                            if (_hubMessageSize == 0 && _defaultSendIntervalSeconds == 0)
                            {
                                // we use also an array for a single message to make backend processing more consistent
                                encodedhubMessage = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes("[" + jsonMessage.ToString() + "]"));
                            }
                            else
                            {
                                // remove the trailing comma and add a closing square bracket
                                hubMessage.SetLength(hubMessage.Length - 1);
                                hubMessage.Write(Encoding.UTF8.GetBytes("]"), 0, 1);
                                encodedhubMessage = new Microsoft.Azure.Devices.Client.Message(hubMessage.ToArray());
                            }
                            if (_hubClient != null)
                            {
                                nextSendTime += TimeSpan.FromSeconds(_defaultSendIntervalSeconds);
                                try
                                {
                                    _sentBytes += encodedhubMessage.GetBytes().Length;
                                    await _hubClient.SendEventAsync(encodedhubMessage);
                                    _sentMessages++;
                                    _sentLastTime = DateTime.UtcNow;
                                    Logger.Debug($"Sending {encodedhubMessage.BodyStream.Length} bytes to hub.");
                                }
                                catch
                                {
                                    _failedMessages++;
                                }

                                // reset the messaage
                                hubMessage.Position = 0;
                                hubMessage.SetLength(0);
                                hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);

                                // if we had not yet buffered the last message because there was not enough space, buffer it now
                                if (needToBufferMessage)
                                {
                                    // add the message and a comma to the buffer
                                    hubMessage.Write(Encoding.UTF8.GetBytes(jsonMessage.ToString()), 0, jsonMessageSize);
                                    hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                }
                            }
                            else
                            {
                                Logger.Information("No hub client available. Dropping messages...");
                            }
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, "Exception while sending message to hub. Dropping message...");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!(e is OperationCanceledException))
                    {

                        Logger.Error(e, "Error while processing monitored item messages.");
                    }
                }
            }
        }

        /// <summary>
        /// Read twin properties.
        /// </summary>
        public async Task<dynamic> GetTwinProperty(string property)
        {
            try
            {
                var twin = await _hubClient.GetTwinAsync();
                var twinProperties = twin.Properties.Desired;
                return twinProperties[property];
            }
            catch (Exception e)
            {
                Logger.Error(e, "Error in GetTwinProperty.)");
                return null;
            }
        }

        private static string _hubConnectionString = string.Empty;
        private static Microsoft.Azure.Devices.Client.TransportType _hubProtocol = Microsoft.Azure.Devices.Client.TransportType.Mqtt_WebSocket_Only;
        private static uint _hubMessageSize = 262144;
        private static int _defaultSendIntervalSeconds = 10;
        private static int _monitoredItemsQueueCapacity = 8192;
        private static long _enqueueCount;
        private static long _enqueueFailureCount;
        private static long _dequeueCount;
        private static long _missedSendIntervalCount;
        private static long _tooLargeCount;
        private static long _sentBytes;
        private static long _sentMessages;
        private static DateTime _sentLastTime;
        private static long _failedMessages;
        private static BlockingCollection<MessageData> _monitoredItemsDataQueue;
        private static Task _monitoredItemsProcessorTask;
        private static DeviceClient _hubClient;
        private static TransportType _transportType;
        private static CancellationToken _shutdownToken;
        private static bool _iotCentralMode = false;
    }
}
