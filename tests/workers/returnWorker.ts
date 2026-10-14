// Returns `payload.value` as it is, for checking that any value a run can
// return - null, undefined, falsy ones - comes back to the host intact.

export default class ReturnWorker {
  async run(payload: any) {
    return payload.value;
  }
}
