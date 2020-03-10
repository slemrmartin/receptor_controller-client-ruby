require "faraday"

module ReceptorController
  class Client::Directive
    attr_accessor :name, :account, :log_message_common, :node_id, :payload, :client

    delegate :config, :logger, :receptor_log_msg, :response_worker, :to => :client

    MESSAGE_TYPE_RESPONSE, MESSAGE_TYPE_EOF = 'response'.freeze, 'eof'.freeze

    def initialize(name:, account:, node_id:, payload:, client:, log_message_common: nil)
      self.account            = account
      self.client             = client
      self.log_message_common = log_message_common
      self.name               = name
      self.node_id            = node_id
      self.payload            = payload
    end

    def call(_body = default_body)
      raise NotImplementedError, "#{__method__} must be implemented in a subclass"
    end

    def default_body
      {
        :account   => account,
        :recipient => node_id,
        :payload   => payload,
        :directive => name
      }
    end
  end
end
