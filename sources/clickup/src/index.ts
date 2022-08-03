import {Command} from 'commander';
import fetch from 'cross-fetch';
import {
  AirbyteConfig,
  AirbyteLogger,
  AirbyteSourceBase,
  AirbyteSourceRunner,
  AirbyteSpec,
  AirbyteStreamBase,
} from 'faros-airbyte-cdk';
import VError from 'verror';

import {Tasks} from './streams';

/** The main entry point. */
export function mainCommand(): Command {
  const logger = new AirbyteLogger();
  const source = new TaskSource(logger);
  return new AirbyteSourceRunner(logger, source).mainCommand();
}

/** Example source implementation. */
class TaskSource extends AirbyteSourceBase {
  async spec(): Promise<AirbyteSpec> {
    return new AirbyteSpec(require('../resources/spec.json'));
  }
  async checkConnection(config: AirbyteConfig): Promise<[boolean, VError]> {
    const listId = config.list_id;
    const personalToken = config.personal_token;
    const res = await fetch(
      `https://api.clickup.com/api/v2/list/${listId}/task`,
      {
        method: 'get',
        headers: {
          Authorization: personalToken,
          'Content-Type': 'application/json',
        },
      }
    );
    if (res.ok) {
      return [true, undefined];
    }
    return [false, new VError('Error in api call')];
  }
  streams(config: AirbyteConfig): AirbyteStreamBase[] {
    return [new Tasks(config, this.logger)];
  }
}
