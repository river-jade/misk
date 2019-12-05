package misk.jobqueue.sqs

import com.google.common.util.concurrent.AbstractIdleService
import misk.ServiceModule
import misk.inject.KAbstractModule
import misk.jobqueue.JobHandler
import misk.jobqueue.QueueName
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.reflect.KClass

/**
 * Install this module to register a handler for an SQS queue.
 */
class AwsSqsJobHandlerModule<T : JobHandler> private constructor(
  private val queueNames: List<QueueName>,
  private val handler: KClass<T>
) : KAbstractModule() {
  override fun configure() {
    newMapBinder<List<QueueName>, JobHandler>().addBinding(queueNames).to(handler.java)
    install(ServiceModule<AwsSqsJobHandlerSubscriptionService>())
  }

  companion object {
    inline fun <reified T : JobHandler> create(queueName: QueueName):
        AwsSqsJobHandlerModule<T> = create(listOf(queueName), T::class)

    inline fun <reified T : JobHandler> createPrioritized(vararg queueNames: QueueName):
        AwsSqsJobHandlerModule<T> = create(queueNames.asList(), T::class)

    @JvmStatic
    fun <T : JobHandler> create(
      queueName: QueueName,
      handlerClass: Class<T>
    ): AwsSqsJobHandlerModule<T> {
      return create(listOf(queueName), handlerClass.kotlin)
    }

    /**
     * Returns a module that registers a handler for an SQS queue.
     */
    fun <T : JobHandler> create(
      queueNames: List<QueueName>,
      handlerClass: KClass<T>
    ): AwsSqsJobHandlerModule<T> {
      return AwsSqsJobHandlerModule(queueNames, handlerClass)
    }
  }
}

@Singleton
internal class AwsSqsJobHandlerSubscriptionService @Inject constructor(
  private val consumer: SqsJobConsumer,
  private val consumerMapping: Map<List<QueueName>, JobHandler>
) : AbstractIdleService() {
  override fun startUp() {
    consumerMapping.forEach { consumer.subscribe(it.key, it.value) }
  }

  override fun shutDown() {}
}
