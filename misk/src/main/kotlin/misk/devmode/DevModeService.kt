package misk.devmode

import com.google.common.util.concurrent.AbstractExecutionThreadService
import com.google.common.util.concurrent.Service
import com.google.inject.Inject
import com.google.inject.Key
import misk.DependentService

internal class DevModeService @Inject internal constructor(
  val behaviors: List<DevModeBehavior>
) : AbstractExecutionThreadService(), DependentService {

  override val consumedKeys: Set<Key<*>> = setOf()
  override val producedKeys: Set<Key<*>> = setOf(Key.get(DevModeService::class.java))

  override fun startUp() {}

  override fun run() {
    while (isRunning) {
      behaviors.forEach { it.run() }
    }
  }

  override fun shutDown() {}
}
