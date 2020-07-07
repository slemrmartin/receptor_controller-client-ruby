require "receptor_controller/client/directive_blocking"

RSpec.describe ReceptorController::Client::DirectiveBlocking do
  # TODO: definitions below contain the same like non-blocking spec
  let(:external_tenant) { '0000001' }
  let(:organization_id) { '000001' }
  let(:identity) do
    {"x-rh-identity" => Base64.strict_encode64({"identity" => {"account_number" => external_tenant, "user" => {"is_org_admin" => true}, "internal" => {"org_id" => organization_id}}}.to_json)}
  end
  let(:headers) do
    {"Content-Type"    => "application/json",
     "Accept"          => "*/*",
     "Accept-Encoding" => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3'}.merge(identity)
  end
  let(:receptor_scheme) { 'http' }
  let(:receptor_host) { 'localhost:9090' }
  let(:receptor_node) { 'testing-receptor' }
  let(:receptor_config) do
    ReceptorController::Client::Configuration.new do |config|
      config.controller_scheme = receptor_scheme
      config.controller_host   = receptor_host
    end
  end
  let(:receptor_client) do
    client = ReceptorController::Client.new(:config => receptor_config)
    client.identity_header = identity
    client
  end

  let(:payload) { {'method' => :get, 'url' => 'tower.example.com', 'headers' => {}, 'ssl' => false}.to_json }
  let(:directive) { 'receptor_http:execute'}

  subject { described_class.new(:name => directive, :account => external_tenant, :node_id => receptor_node, :payload => payload, :client => receptor_client) }

  describe "#call" do
    it "makes POST /job request to receptor and registers received message ID" do
      allow(subject).to receive(:wait_for_response)

      response = {"id" => '1234'}

      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => subject.default_body.to_json,
              :headers => headers)
        .to_return(:status => 200, :body => response.to_json, :headers => {})

      expect(subject.response_worker).to receive(:register_message).with(response['id'], subject)

      subject.call
    end

    it "raises ControllerResponseError if POST /job returns error" do
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => subject.default_body.to_json,
              :headers => headers)
        .to_return(:status => 401, :body => {"errors" => [{"status" => 401, "detail" => "Unauthorized"}]}.to_json, :headers => {})

      expect(subject.response_worker).not_to receive(:register_message)

      expect {subject.call}.to raise_error(ReceptorController::Client::ControllerResponseError)
    end

    context "waiting for callbacks" do
      let(:http_response) { {"id" => '1234'} }
      let(:kafka_client) { double("Kafka client") }
      let(:kafka_response) { double("Kafka response") }

      before do
        stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
          .with(:body    => subject.default_body.to_json,
                :headers => headers)
          .to_return(:status => 200, :body => http_response.to_json, :headers => {})

        allow(kafka_response).to receive(:ack)

        allow(ManageIQ::Messaging::Client).to receive(:open).and_return(kafka_client)
        allow(kafka_client).to receive(:subscribe_topic).and_yield(kafka_response)
        allow(kafka_client).to receive(:close)

      end

      it "waits for successful response and sets data" do
        response_message = {'code'           => 0,
                            'in_response_to' => http_response['id'],
                            'message_type'   => subject.class::MESSAGE_TYPE_RESPONSE,
                            'payload'        => 'Test payload'}

        allow(kafka_response).to receive(:payload).and_return(response_message.to_json)

        expect(subject).to receive(:response_success)
                             .with(response_message['in_response_to'],
                                   response_message['message_type'],
                                   response_message['payload'])
                             .and_wrap_original do |m, *args|
          m.call(*args) # Doesn't release lock, only EOF response can
          subject.send(:response_waiting).signal
          subject.response_worker.stop
        end

        subject.response_worker.start
        subject.call

        expect(subject.send(:response_data)).to eq(response_message['payload'])
        expect(subject.send(:response_exception)).to be_nil
      end

      it "waits for EOF response" do
        response_message = {'code'           => 0,
                            'in_response_to' => http_response['id'],
                            'message_type'   => subject.class::MESSAGE_TYPE_EOF,
                            'payload'        => 'Unimportant'}

        allow(kafka_response).to receive(:payload).and_return(response_message.to_json)

        expect(subject).to receive(:response_success)
                             .with(response_message['in_response_to'],
                                   response_message['message_type'],
                                   response_message['payload'])
                             .and_wrap_original do |m, *args|
          m.call(*args) # original call releases lock
          subject.response_worker.stop
        end

        subject.response_worker.start
        subject.call

        expect(subject.send(:response_data)).to be_nil
        expect(subject.send(:response_exception)).to be_nil
      end

      it "raises UnknownResponseTypeError if unknown successful response received" do
        response_message = {'code'           => 0,
                            'in_response_to' => http_response['id'],
                            'message_type'   => 'unknown',
                            'payload'        => 'Unimportant'}

        allow(kafka_response).to receive(:payload).and_return(response_message.to_json)

        expect(subject).to receive(:response_success)
                             .with(response_message['in_response_to'],
                                   response_message['message_type'],
                                   response_message['payload'])
                             .and_wrap_original do |m, *args|
          m.call(*args) # original call releases lock
          subject.response_worker.stop
        end

        subject.response_worker.start
        expect { subject.call }.to raise_error(ReceptorController::Client::UnknownResponseTypeError)

        expect(subject.send(:response_data)).to be_nil
        expect(subject.send(:response_exception)).to be_kind_of(ReceptorController::Client::UnknownResponseTypeError)
      end

      it "raises ResponseError if error response received" do
        response_message = {'code'           => 1,
                            'in_response_to' => http_response['id'],
                            'payload'        => 'Some error message'}

        allow(kafka_response).to receive(:payload).and_return(response_message.to_json)

        expect(subject).to receive(:response_error)
                             .with(response_message['in_response_to'],
                                   response_message['code'],
                                   response_message['payload'])
                             .and_wrap_original do |m, *args|
          m.call(*args) # original calls releases lock
          subject.response_worker.stop
        end

        subject.response_worker.start
        expect { subject.call }.to raise_error(ReceptorController::Client::ResponseError)

        expect(subject.send(:response_data)).to be_nil
        expect(subject.send(:response_exception)).to be_kind_of(ReceptorController::Client::ResponseError)
      end

      it "raises ResponseTimeoutError if timeout response received" do
        receptor_client.config.response_timeout = 1.second
        receptor_client.config.response_timeout_poll_time = 0

        allow(kafka_client).to receive(:subscribe_topic).and_return(nil)

        expect(subject).to receive(:response_timeout)
                             .with(http_response['id'])
                             .and_wrap_original do |m, *args|
          m.call(*args) # original call releases lock
          subject.response_worker.stop
        end

        subject.response_worker.start
        expect { subject.call }.to raise_error(ReceptorController::Client::ResponseTimeoutError)

        expect(subject.send(:response_data)).to be_nil
        expect(subject.send(:response_exception)).to be_kind_of(ReceptorController::Client::ResponseTimeoutError)
      end
    end
  end
end
