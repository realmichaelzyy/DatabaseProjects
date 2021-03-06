from werkzeug.local import LocalProxy
import ujson

from myfecviz import get_db

db = LocalProxy(get_db)


def get_number_of_candidates():
    """Return the number of candidates registered with the FEC.

    This serves as just an example query.

    :returns: integer
    """
    # Execute database query
    db.execute("SELECT COUNT(*) FROM candidates;")
    results = db.fetchall()

    # Package into output
    return int(results[0][0])


def get_all_transaction_amounts():
    """Return all transaction amounts with the state that the contribution came from.

    For all committee contributions with a transaction_amt greater than zero,
    return every transaction amount with the state that the contribution came form.

    :return: List of dictionaries with 'state' and 'amount' keys
    """
    #raise NotImplementedError('Needs implementation')
    # Execute database query

    # Package into output
    db.execute("select H.transaction_amt, H.state from committee_contributions H where H.transaction_amt > 0;")
    results = db.fetchall()
    returnResult = []
    for item in results:
        returnResult.append({'amount':item[0], 'state':item[1]})

    return returnResult


def get_total_transaction_amounts_by_state():
    """Return a list of dicts containing the state and total contributions.

    For all committee contributions with a transaction_amt greater than zero,
    return a dictionary containing the state and total amount.

    :returns: List of dictionaries with 'state' and 'total_amount' keys
    """
    #raise NotImplementedError('Needs implementation')
    # Execute database query

    # Package into list of dictionaries
    db.execute("select sum(H.transaction_amt), H.state from committee_contributions H where H.transaction_amt > 0 group by H.state;")
    results = db.fetchall()
    returnResult = []
    for item in results:
        returnResult.append({'total_amount':item[0], 'state':item[1]})
    return returnResult
