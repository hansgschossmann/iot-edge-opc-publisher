﻿
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Security.Cryptography.X509Certificates;
    using static Program;

    /// <summary>
    /// Class to handle all IoTEdge communication.
    /// </summary>
    public class IotEdgeHubCommunication : HubCommunication
    {
        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public IotEdgeHubCommunication(CancellationToken ct) : base(ct)
        {
        }

        /// <summary>
        /// Add certificate in local cert store for use by client for secure connection to IoT Edge runtime
        /// </summary>
        static void InstallEdgeHubCert()
        {
            string certPath = Environment.GetEnvironmentVariable("EdgeModuleCACertificateFile");
            Logger.Information($"edgeModule certificate file is: {certPath}");
            if (string.IsNullOrWhiteSpace(certPath))
            {
                // We cannot proceed further without a proper cert file
                Logger.Information($"Missing path to certificate collection file: {certPath}");
                throw new InvalidOperationException("Missing path to certificate file.");
            }
            else if (!File.Exists(certPath))
            {
                // We cannot proceed further without a proper cert file
                Logger.Error($"Missing path to certificate collection file: {certPath}");
                throw new InvalidOperationException("Missing certificate file.");
            }
            X509Store store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            store.Add(new X509Certificate2(X509Certificate2.CreateFromCertFile(certPath)));
            Logger.Information($"Added IoT EdgeHub Cert (${certPath}) to X509 store.");
            store.Close();
        }


        /// <summary>
        /// Initializes the EdgeHub communication.
        /// </summary>
        public async Task<bool> InitAsync()
        {
            try
            {
                // read the EdgeHub connection string from the environment
                string edgeHubConnectionString = Environment.GetEnvironmentVariable("EdgeHubConnectionString");
                Logger.Information($"Using EdgeHubConnectionString '{edgeHubConnectionString}'.");

                // we also need to initialize the cert verification, but it is not yet fully functional under Windows
                // Cert verification is not yet fully functional when using Windows OS for the container
                bool bypassCertVerification = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
                if (!bypassCertVerification)
                {
                    InstallEdgeHubCert();
                }

                MqttTransportSettings mqttSettings = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);

                // during dev you might want to bypass the cert verification. It is highly recommended to verify certs systematically in production
                if (bypassCertVerification)
                {
                    Logger.Information($"ATTENTION: You are bypassing the IoTEdgeHub security cert verfication. Please ensure this was intentional.");
                    mqttSettings.RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
                }
                ITransportSettings[] transportSettings = { mqttSettings };

                // connect to EdgeHub
                HubProtocol = TransportType.Mqtt_Tcp_Only;
                Logger.Information($"Create IoTEdgeHub client with connection string using '{HubProtocol}' for communication.");
                ModuleClient hubClient = ModuleClient.CreateFromConnectionString(edgeHubConnectionString, transportSettings);

                if (await InitHubCommunicationAsync(hubClient, TransportType.Mqtt_Tcp_Only))
                {
                    return true;
                }
                return false;
            }
            catch (Exception e)
            {
                Logger.Error(e, "Error in IoTEdgeHub initialization.)");
                return false;
            }
        }
    }
}
