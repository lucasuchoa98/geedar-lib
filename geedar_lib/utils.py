def which(self):
    """
    Essa função é uma adaptação da função "with" da linguagem R
    """
    try:
        self = list(iter(self))
    except TypeError as e:
        raise Exception("""'which' method can only be applied to iterables.
        {}""".format(str(e)))
    indices = [i for i, x in enumerate(self) if bool(x) == True]
    return(indices)


def writeToLogFile(lines, entryType, identifier):
    """
    Essa função escreve um log file
    """

    log_file = "GEEDaR_log.txt"

    if not isinstance(lines, list):
        lines = [lines]
    try:
        dateAndTime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        f = open(log_file, "a")
        for line in lines:
            f.write(dateAndTime + "," + str(entryType) + "," + str(identifier) + "," + line + "\n")
        f.close()
    except:
        print("(!)")
        print("The message(s) below could not be written to the log file (" + log_file + "):")
        for line in lines:
            print(line)
        print("(.)")