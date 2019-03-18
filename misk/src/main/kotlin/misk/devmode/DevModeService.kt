package misk.devmode

import com.google.common.util.concurrent.AbstractIdleService
import com.google.inject.Inject
import com.google.inject.Key
import com.google.inject.Singleton
import misk.DependentService
import java.lang.Thread.sleep

@Singleton
internal class DevModeService @Inject internal constructor(
  val behaviors: List<DevModeBehavior>
) : AbstractIdleService(), DependentService {

  override val consumedKeys: Set<Key<*>> = setOf()
  override val producedKeys: Set<Key<*>> = setOf(Key.get(DevModeService::class.java))

  override fun startUp() {
    println("Dev Mode Running...")
    sleep(5)
    println("Dev Mode Done...")
    behaviors.forEach { it.run() }
  }

  override fun shutDown() {}
}
