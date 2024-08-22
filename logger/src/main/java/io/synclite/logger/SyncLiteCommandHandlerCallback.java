package io.synclite.logger;

import java.nio.file.Path;

public interface SyncLiteCommandHandlerCallback {
	void handleCommand(String command, Path commandFile);
}
