import fetch from 'cross-fetch';
import {
  AirbyteConfig,
  AirbyteLogger,
  AirbyteStreamBase,
  StreamKey,
  SyncMode,
} from 'faros-airbyte-cdk';
import _ from 'lodash';
import {Dictionary} from 'ts-essentials';

export type Task = {
  id: string;
  name: string;
  description: string;
  date_created: number;
  date_updated: number;
  custom_fields: object[];
};

export type TaskState = {
  date_updated: number;
};

export class Tasks extends AirbyteStreamBase {
  constructor(
    private readonly config: AirbyteConfig,
    protected readonly logger: AirbyteLogger
  ) {
    super(logger);
  }
  getJsonSchema(): Dictionary<any, string> {
    return require('../../resources/schemas/tasks.json');
  }
  get primaryKey(): StreamKey {
    return 'id';
  }
  get cursorField(): string {
    return 'date_updated';
  }

  async *readRecords(
    syncMode: SyncMode,
    cursorField?: string[],
    streamSlice?: Dictionary<any, string>,
    streamState?: TaskState
  ): AsyncGenerator<Dictionary<any, string>, any, unknown> {
    const state = syncMode === SyncMode.INCREMENTAL ? streamState : undefined;
    const cursField = state?.date_updated;
    const res = this.getTasks(Number(cursField ? cursField : 0));
    yield* res;
  }

  getUpdatedState(
    currentStreamState: TaskState,
    latestRecord: Task
  ): TaskState {
    const dateUpdated = currentStreamState?.date_updated
      ? currentStreamState.date_updated
      : 0;
    const taskUpdated = latestRecord?.date_updated
      ? latestRecord.date_updated
      : 0;
    return {
      date_updated: _.max([dateUpdated, taskUpdated]),
    };
  }

  async *getTasks(updatetGreatThan: number): AsyncGenerator<Task> {
    let page = 0;
    let tasksLength = 0;
    const listId = this.config.list_id;
    const personalToken = this.config.personal_token;
    do {
      let url = `https://api.clickup.com/api/v2/list/${listId}/task?page=${page}`;
      if (updatetGreatThan) {
        url += `&date_updated_gt=${updatetGreatThan}`;
      }
      const res = await fetch(url, {
        method: 'get',
        headers: {
          Authorization: personalToken,
          'Content-Type': 'application/json',
        },
      });
      page++;
      const resp = await res.json();
      const tasks = resp['tasks'] as Task[];
      for (const task of tasks) {
        yield {
          id: task.id,
          date_created: Number(task.date_created),
          date_updated: Number(task.date_updated),
          name: task.name,
          description: task.description,
          custom_fields: task.custom_fields,
        };
      }
      tasksLength = tasks.length;
    } while (tasksLength);
  }
}
