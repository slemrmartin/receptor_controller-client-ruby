require "faraday"
require "manageiq-messaging"

module ReceptorController
  class Client
    require "topological_inventory/satellite/receptor/client/configuration"
    require "topological_inventory/satellite/receptor/client/response_worker"

    attr_accessor :default_headers, :identity_header, :logger
    attr_reader :config

    STATUS_DISCONNECTED = {'status' => 'disconnected'}.freeze

    class << self
      def configure
        if block_given?
          yield(Configuration.default)
        else
          Configuration.default
        end
      end
    end
    delegate :start, :stop, :to => :response_worker

    def initialize(config: Configuration.default, logger: ManageIQ::Messaging::NullLogger.new)
      self.config          = config
      self.default_headers = {"Content-Type" => "application/json"}
      self.logger          = logger
      self.response_worker = ResponseWorker.new(config, logger)
    end

    def connection_status(account_number, node_id)
      body = {
        :account => account_number,
        :node_id => node_id
      }.to_json

      response = Faraday.post(config.connection_status_url, body, headers)
      if response.success?
        JSON.parse(response.body)
      else
        logger.error(receptor_log_msg("Connection_status failed: HTTP #{response.status}", account_number, node_id))
        STATUS_DISCONNECTED
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Connection_status failed", account_number, node_id, e))
      STATUS_DISCONNECTED
    end

    def send_directive(account_number, node_id,
                       payload:,
                       directive:,
                       response_object:,
                       response_callback: :response_received,
                       timeout_callback: :response_timeout)
      body = {
        :account   => account_number,
        :recipient => node_id,
        :payload   => payload,
        :directive => directive
      }

      response = Faraday.post(config.job_url, body.to_json, headers)
      if response.success?
        msg_id = JSON.parse(response.body)['id']

        # registers message id for kafka responses
        response_worker.register_message(msg_id,
                                         response_object,
                                         :response_callback => response_callback,
                                         :timeout_callback  => timeout_callback)

        msg_id
      else
        logger.error(receptor_log_msg("Directive #{directive} failed: HTTP #{response.status}", account_number, node_id))
        nil
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Directive #{directive} failed", account_number, node_id, e))
      nil
    end

    private

    attr_writer :config
    attr_accessor :response_worker

    def headers
      default_headers.merge(identity_header || {})
    end

    def receptor_log_msg(msg, account, node_id, exception = nil)
      message = "Receptor: #{msg}"
      message += "; #{exception.class.name}: #{exception.message}" if exception.present?
      message + " [Account number: #{account}; Receptor node: #{node_id}]"
    end
  end
end
