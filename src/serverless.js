const aws = require('aws-sdk')
const { isEmpty, mergeDeepRight, pick } = require('ramda')
const { Component } = require('@serverless/core')
const {
  createQueue,
  deleteQueue,
  getDefaults,
  getQueue,
  getAccountId,
  getArn,
  getUrl,
  setAttributes,
  createEventSourceMapping,
  deleteEventSourceMapping,
  getClients
} = require('./utils')

const outputsList = ['arn', 'url']

const defaults = {
  name: 'serverless',
  region: 'eu-central-1'
}

class AwsSqsQueue extends Component {
  async deploy(inputs = {}) {
    const config = mergeDeepRight(getDefaults({ defaults }), inputs)

    // Get AWS clients
    const { sqs, lambda, sts } = getClients(this.credentials.aws, config.region)

    const accountId = await getAccountId(sts)

    const arn = getArn({
      aws,
      accountId,
      name: config.name,
      region: config.region
    })

    const queueUrl = getUrl({
      aws,
      accountId,
      name: config.name,
      region: config.region
    })

    config.arn = arn
    config.url = queueUrl

    const { functionName, batchSize = 1 } = config

    console.log(`Deploying`)

    const prevInstance = await getQueue({ sqs, queueUrl: this.state.url || queueUrl })

    console.debug(`arn: ${arn}`)
    console.debug(`functionName: ${functionName}`)

    if (isEmpty(prevInstance)) {
      console.log(`Creating`)
      await createQueue({
        sqs,
        config: config
      })

      if (functionName) {
        console.debug(`Creating lambda event source mapping`)
        const { UUID: eventSourceMappingUuid } = await createEventSourceMapping({
          lambda,
          functionName,
          batchSize,
          sqsArn: arn
        })

        this.state.eventSourceMappingUuid = eventSourceMappingUuid
        this.state.functionName = functionName
      }
    } else {
      if (this.state.url === queueUrl) {
        console.log(`Updating`)
        await setAttributes(sqs, queueUrl, config)
      } else {
        console.debug(`The QueueUrl has changed`)

        console.debug(`Deleting previous queue`)

        await deleteQueue({ sqs, queueUrl: this.state.url })

        console.debug(`Creating new queue`)
        await createQueue({
          sqs,
          config: config
        })
      }

      if (this.state.functionName !== functionName) {
        if (this.state.eventSourceMappingUuid) {
          await deleteEventSourceMapping({ lambda, uuid: this.state.eventSourceMappingUuid })
          this.state.eventSourceMappingUuid = undefined
          this.state.functionName = undefined
        }

        if (functionName) {
          console.debug(`Creating lambda event source mapping`)

          const { UUID: eventSourceMappingUuid } = await createEventSourceMapping({
            lambda,
            functionName,
            batchSize,
            sqsArn: arn
          })

          this.state.eventSourceMappingUuid = eventSourceMappingUuid
          this.state.functionName = functionName
        }
      }
    }

    this.state.name = config.name
    this.state.arn = config.arn
    this.state.url = config.url
    await this.save()

    const outputs = pick(outputsList, config)
    return outputs
  }

  async remove(inputs = {}) {
    console.debug(`inputs: `, JSON.stringify(inputs))
    const config = mergeDeepRight(getDefaults({ defaults }), inputs)
    config.name = inputs.name || this.state.name || defaults.name

    const { sqs, lambda, sts } = getClients(this.credentials.aws, config.region)

    const accountId = await getAccountId(sts)

    const queueUrl =
      this.state.url ||
      getUrl({
        aws,
        accountId,
        name: config.name,
        region: config.region
      })

    console.log(`Removing`)

    console.debug(`credentials: ${JSON.stringify(this.credentials.aws)}`)
    console.debug(`config: ${JSON.stringify(config)}`)

    const { EventSourceMappings } = await lambda
      .listEventSourceMappings({
        FunctionName: this.state.functionName
      })
      .promise()
      .then((res) => {
        console.debug('List EventSourceMappings:')
        console.debug(res)
        return res
      })

    console.debug(`uuid: ${this.state.eventSourceMappingUuid}`)
    if (this.state.eventSourceMappingUuid) {
      console.log('Deleting event source mapping...')

      await Promise.all(
        EventSourceMappings.map(async ({ UUID }) => {
          console.debug(`EventSourceMappingUUID: ${UUID}`)

          await deleteEventSourceMapping({ lambda, uuid: UUID })
            .then(console.debug)
            .catch((error) => {
              console.log(`Error: ${error.code}`)
              if (error.code !== 'ResourceNotFoundException') {
                throw error
              }
            })
        })
      )
    }

    console.log('Deleting queue...')

    await deleteQueue({ sqs, queueUrl })

    this.state = {}
    await this.save()

    return {}
  }
}

module.exports = AwsSqsQueue
